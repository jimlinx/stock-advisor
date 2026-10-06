"""Shared login for the status and stock pages: one users file, one password per person.

The users file (caddy/auth.conf) is one `name bcrypt-hash [sites]` line per person; sites, when given, is a
comma list of the apps that person may sign in to (`chris <hash> stock`: the stock app only). Caddy no longer
reads it; the two web apps do, and set_password() rewrites it, so a change on either page
applies to both. Sessions stay per app (each site has its own cookie).
"""
import os, secrets, sqlite3, tempfile, threading, time
import bcrypt

AUTH_FILE = '/opt/stock-advisor/auth.conf'
MIN_LEN = 8
SESSION_TTL = 30 * 86400
LOCKOUT = (10, 900)  # 10 failed logins from one address within 15 minutes locks that address out
_lock = threading.Lock()
_fails = {}  # client address -> failure times; in memory, so a restart clears lockouts


def lines():
    try:
        with open(AUTH_FILE) as f:
            return [l.split() for l in f if l.strip() and not l.startswith('#')]
    except OSError:
        return []


def users():
    return {l[0]: l[1] for l in lines()}


def sites(user):
    """The apps `user` may sign in to, or None for every app."""
    return next((set(l[2].split(',')) if len(l) > 2 else None for l in lines() if l[0] == user), None)


def check_password(user, password, site=None):
    h = users().get(user)
    ok = bool(h) and bcrypt.checkpw(password.encode(), h.encode())
    return ok and (site is None or sites(user) is None or site in sites(user))


def set_password(user, password):
    """Replace one user's hash, keeping comments and other users. Atomic; mode 600."""
    if user not in users():
        raise ValueError('unknown user')
    if len(password) < MIN_LEN:
        raise ValueError(f'password must be at least {MIN_LEN} characters')
    h = bcrypt.hashpw(password.encode(), bcrypt.gensalt(10)).decode()  # cost 10: one shared vCPU
    with _lock:
        with open(AUTH_FILE) as f:
            rows = f.read().splitlines()
        out = [' '.join([user, h] + l.split()[2:]) if l.split()[:1] == [user] and not l.startswith('#') else l for l in rows]
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(AUTH_FILE))
        with os.fdopen(fd, 'w') as f:
            f.write('\n'.join(out) + '\n')
        os.chmod(tmp, 0o600)
        os.replace(tmp, AUTH_FILE)


def locked(ip):
    t = time.time()
    _fails[ip] = [x for x in _fails.get(ip, []) if t - x < LOCKOUT[1]]
    return len(_fails[ip]) >= LOCKOUT[0]


def failed(ip):
    _fails.setdefault(ip, []).append(time.time())


def cleared(ip):
    _fails.pop(ip, None)


class Conn(sqlite3.Connection):
    """`with` commits AND closes. Plain sqlite3 only commits, so every such block leaked a connection."""
    def __exit__(self, *a):
        try:
            return super().__exit__(*a)
        finally:
            self.close()


class Sessions:
    """Random tokens in SQLite, sliding TTL. Same behaviour as the stock app's own sessions."""

    def __init__(self, path):
        self.path = path
        with self._db() as c:
            c.execute('CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user TEXT, expires REAL)')

    def _db(self):
        return sqlite3.connect(self.path, timeout=10, factory=Conn)

    def new(self, user):
        token = secrets.token_urlsafe(32)
        with self._db() as c:
            c.execute('DELETE FROM sessions WHERE expires < ?', (time.time(),))
            c.execute('INSERT INTO sessions VALUES (?,?,?)', (token, user, time.time() + SESSION_TTL))
        return token

    def user(self, token):
        if not token:
            return None
        with self._db() as c:
            r = c.execute('SELECT user, expires FROM sessions WHERE token=?', (token,)).fetchone()
            if not r or r[1] < time.time():
                return None
            if r[1] - time.time() < SESSION_TTL - 3600:  # slide, at most one write an hour
                c.execute('UPDATE sessions SET expires=? WHERE token=?', (time.time() + SESSION_TTL, token))
        return r[0]

    def end(self, token):
        with self._db() as c:
            c.execute('DELETE FROM sessions WHERE token=?', (token or '',))

    def end_others(self, user, keep):
        with self._db() as c:
            c.execute('DELETE FROM sessions WHERE user=? AND token<>?', (user, keep or ''))


if __name__ == '__main__':  # self-check on a copy of the users file
    import shutil
    d = tempfile.mkdtemp()
    AUTH_FILE = os.path.join(d, 'auth.conf')
    with open(AUTH_FILE, 'w') as f:
        f.write('# note\nalex ' + bcrypt.hashpw(b'aaaa1111', bcrypt.gensalt(4)).decode() + '\nsam '
                + bcrypt.hashpw(b'bbbb2222', bcrypt.gensalt(4)).decode() + '\nchris '
                + bcrypt.hashpw(b'dddd4444', bcrypt.gensalt(4)).decode() + ' stock\n')
    assert check_password('chris', 'dddd4444', 'stock') and not check_password('chris', 'dddd4444', 'status')
    assert check_password('sam', 'bbbb2222', 'status')
    set_password('chris', 'eeee5555')
    assert sites('chris') == {'stock'} and check_password('chris', 'eeee5555', 'stock')
    assert check_password('alex', 'aaaa1111') and not check_password('alex', 'x')
    set_password('alex', 'cccc3333')
    assert check_password('alex', 'cccc3333') and check_password('sam', 'bbbb2222')
    assert open(AUTH_FILE).read().startswith('# note\n') and oct(os.stat(AUTH_FILE).st_mode)[-3:] == '600'
    for bad in (('nobody', 'longenough'), ('alex', 'short')):
        try:
            set_password(*bad); assert 0
        except ValueError:
            pass
    s = Sessions(os.path.join(d, 's.db')); t = s.new('sam'); t2 = s.new('sam')
    assert s.user(t) == 'sam'; s.end_others('sam', t); assert s.user(t2) is None and s.user(t) == 'sam'
    s.end(t); assert s.user(t) is None
    shutil.rmtree(d); print('ok')
