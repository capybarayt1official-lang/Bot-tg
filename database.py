import psycopg2
import psycopg2.extras
from config import DATABASE_URL

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            capybara_name TEXT UNIQUE,
            coins INTEGER DEFAULT 150,
            referrals_count INTEGER DEFAULT 0,
            referred_by BIGINT DEFAULT NULL,
            clan_id INTEGER DEFAULT NULL,
            clan_role TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            work_started_at TIMESTAMP DEFAULT NULL,
            last_collected_at TIMESTAMP DEFAULT NULL,
            is_working BOOLEAN DEFAULT FALSE,
            last_daily TIMESTAMP DEFAULT NULL,
            last_battle TIMESTAMP DEFAULT NULL,
            battles_won INTEGER DEFAULT 0,
            equipped_weapon TEXT DEFAULT NULL,
            equipped_armor TEXT DEFAULT NULL,
            base_attack INTEGER DEFAULT 100,
            base_health INTEGER DEFAULT 100,
            stars_spent INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS clans (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            owner_id BIGINT NOT NULL,
            treasury INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS clan_invites (
            id SERIAL PRIMARY KEY,
            clan_id INTEGER NOT NULL,
            invited_user_id BIGINT NOT NULL,
            invited_by BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS inventory (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            item_type TEXT NOT NULL,
            item_name TEXT NOT NULL,
            item_emoji TEXT NOT NULL,
            bonus INTEGER NOT NULL,
            acquired_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS battle_requests (
            id SERIAL PRIMARY KEY,
            challenger_id BIGINT NOT NULL,
            target_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cols = [
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS stars_spent INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily TIMESTAMP DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_battle TIMESTAMP DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS battles_won INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS equipped_weapon TEXT DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS equipped_armor TEXT DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS base_attack INTEGER DEFAULT 100",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS base_health INTEGER DEFAULT 100",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS work_started_at TIMESTAMP DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_collected_at TIMESTAMP DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_working BOOLEAN DEFAULT FALSE",
    ]
    for col in cols:
        try:
            cur.execute(col)
            conn.commit()
        except:
            conn.rollback()
    cur.close()
    conn.close()

def get_user(user_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

def get_user_by_username(username):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

def get_user_by_capybara_name(name):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM users WHERE LOWER(capybara_name) = LOWER(%s)", (name,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

def create_user(user_id, username, capybara_name, referred_by=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (user_id, username, capybara_name, coins, referred_by)
        VALUES (%s, %s, %s, 150, %s) ON CONFLICT DO NOTHING
    """, (user_id, username, capybara_name, referred_by))
    if referred_by:
        cur.execute("SELECT user_id FROM users WHERE user_id = %s", (referred_by,))
        if cur.fetchone():
            cur.execute("UPDATE users SET coins = coins + 120, referrals_count = referrals_count + 1 WHERE user_id = %s", (referred_by,))
            cur.execute("UPDATE users SET coins = coins + 80 WHERE user_id = %s", (user_id,))
    conn.commit()
    cur.close(); conn.close()

def update_capybara_name(user_id, new_name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET capybara_name = %s WHERE user_id = %s", (new_name, user_id))
    conn.commit()
    cur.close(); conn.close()

def add_coins(user_id, amount):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s", (amount, user_id))
    conn.commit()
    cur.close(); conn.close()

def add_stars(user_id, amount):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET stars_spent = stars_spent + %s WHERE user_id = %s", (amount, user_id))
    conn.commit()
    cur.close(); conn.close()

def start_work(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_working = TRUE, work_started_at = NOW(), last_collected_at = NOW() WHERE user_id = %s", (user_id,))
    conn.commit()
    cur.close(); conn.close()

def collect_work(user_id):
    import random
    from datetime import datetime
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
    user = cur.fetchone()
    if not user or not user['is_working']:
        cur.close(); conn.close()
        return 0, 0
    last = user['last_collected_at'] or user['work_started_at']
    now = datetime.now()
    diff_minutes = (now - last).total_seconds() / 60
    periods = int(diff_minutes / 30)
    if periods <= 0:
        cur.close(); conn.close()
        return 0, int(30 - diff_minutes)
    earned = sum(random.randint(30, 90) for _ in range(periods))
    cur2 = conn.cursor()
    cur2.execute("UPDATE users SET coins = coins + %s, last_collected_at = NOW() WHERE user_id = %s", (earned, user_id))
    conn.commit()
    cur.close(); cur2.close(); conn.close()
    return earned, 0

def stop_work(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_working = FALSE, work_started_at = NULL, last_collected_at = NULL WHERE user_id = %s", (user_id,))
    conn.commit()
    cur.close(); conn.close()

def claim_daily(user_id):
    import random
    from datetime import datetime
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT last_daily FROM users WHERE user_id = %s", (user_id,))
    user = cur.fetchone()
    now = datetime.now()
    if user['last_daily']:
        diff = (now - user['last_daily']).total_seconds() / 3600
        if diff < 12:
            cur.close(); conn.close()
            return 0, int(12 - diff)
    reward = random.randint(30, 90)
    cur2 = conn.cursor()
    cur2.execute("UPDATE users SET coins = coins + %s, last_daily = NOW() WHERE user_id = %s", (reward, user_id))
    conn.commit()
    cur.close(); cur2.close(); conn.close()
    return reward, 0

def add_item(user_id, item_type, item_name, item_emoji, bonus):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO inventory (user_id, item_type, item_name, item_emoji, bonus) VALUES (%s, %s, %s, %s, %s)",
                (user_id, item_type, item_name, item_emoji, bonus))
    conn.commit()
    cur.close(); conn.close()

def get_inventory(user_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM inventory WHERE user_id = %s ORDER BY bonus DESC", (user_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def equip_item(user_id, item_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM inventory WHERE id = %s AND user_id = %s", (item_id, user_id))
    item = cur.fetchone()
    if not item:
        cur.close(); conn.close()
        return None
    cur2 = conn.cursor()
    if item['item_type'] == 'weapon':
        cur2.execute("UPDATE users SET equipped_weapon = %s WHERE user_id = %s",
                     (f"{item['item_emoji']} {item['item_name']} (+{item['bonus']}%)", user_id))
    else:
        cur2.execute("UPDATE users SET equipped_armor = %s WHERE user_id = %s",
                     (f"{item['item_emoji']} {item['item_name']} (+{item['bonus']}%)", user_id))
    conn.commit()
    cur.close(); cur2.close(); conn.close()
    return item

def get_stats(user_id):
    user = get_user(user_id)
    attack = user['base_attack']
    health = user['base_health']
    if user['equipped_weapon']:
        try:
            bonus = int(user['equipped_weapon'].split('+')[1].replace('%)', ''))
            attack = int(attack * (1 + bonus / 100))
        except:
            pass
    if user['equipped_armor']:
        try:
            bonus = int(user['equipped_armor'].split('+')[1].replace('%)', ''))
            health = int(health * (1 + bonus / 100))
        except:
            pass
    return attack, health

def create_battle_request(challenger_id, target_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM battle_requests WHERE challenger_id = %s OR target_id = %s", (challenger_id, challenger_id))
    cur.execute("INSERT INTO battle_requests (challenger_id, target_id) VALUES (%s, %s)", (challenger_id, target_id))
    conn.commit()
    cur.close(); conn.close()

def get_battle_request(target_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM battle_requests WHERE target_id = %s ORDER BY created_at DESC LIMIT 1", (target_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

def delete_battle_request(target_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM battle_requests WHERE target_id = %s", (target_id,))
    conn.commit()
    cur.close(); conn.close()

def record_battle_win(winner_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET battles_won = battles_won + 1, last_battle = NOW() WHERE user_id = %s", (winner_id,))
    conn.commit()
    cur.close(); conn.close()

def set_last_battle(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET last_battle = NOW() WHERE user_id = %s", (user_id,))
    conn.commit()
    cur.close(); conn.close()

def can_battle(user_id):
    from datetime import datetime
    user = get_user(user_id)
    if not user['last_battle']:
        return True, 0
    diff = (datetime.now() - user['last_battle']).total_seconds() / 3600
    if diff >= 3:
        return True, 0
    return False, int((3 - diff) * 60)

def get_top_users_coins(limit=10):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT capybara_name, coins FROM users ORDER BY coins DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def get_top_users_referrals(limit=10):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT capybara_name, referrals_count FROM users ORDER BY referrals_count DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def get_top_users_battles(limit=10):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT capybara_name, battles_won FROM users ORDER BY battles_won DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def get_top_users_stars(limit=10):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT capybara_name, stars_spent FROM users ORDER BY stars_spent DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def get_clan(clan_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM clans WHERE id = %s", (clan_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

def get_clan_by_name(name):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM clans WHERE name = %s", (name,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

def create_clan(name, owner_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("INSERT INTO clans (name, owner_id) VALUES (%s, %s) RETURNING *", (name, owner_id))
    clan = cur.fetchone()
    cur.execute("UPDATE users SET clan_id = %s, clan_role = 'owner', coins = coins - 200 WHERE user_id = %s", (clan['id'], owner_id))
    conn.commit()
    cur.close(); conn.close()
    return clan

def delete_clan(clan_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET clan_id = NULL, clan_role = NULL WHERE clan_id = %s", (clan_id,))
    cur.execute("DELETE FROM clan_invites WHERE clan_id = %s", (clan_id,))
    cur.execute("DELETE FROM clans WHERE id = %s", (clan_id,))
    conn.commit()
    cur.close(); conn.close()

def leave_clan(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET clan_id = NULL, clan_role = NULL WHERE user_id = %s", (user_id,))
    conn.commit()
    cur.close(); conn.close()

def donate_to_clan(user_id, clan_id, amount):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET coins = coins - %s WHERE user_id = %s", (amount, user_id))
    cur.execute("UPDATE clans SET treasury = treasury + %s WHERE id = %s", (amount, clan_id))
    conn.commit()
    cur.close(); conn.close()

def get_clan_members(clan_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM users WHERE clan_id = %s", (clan_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def get_top_clans():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT c.*, COUNT(u.user_id) as member_count
        FROM clans c LEFT JOIN users u ON u.clan_id = c.id
        GROUP BY c.id ORDER BY member_count DESC LIMIT 10
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def create_invite(clan_id, invited_user_id, invited_by):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM clan_invites WHERE clan_id = %s AND invited_user_id = %s", (clan_id, invited_user_id))
    if cur.fetchone():
        cur.close(); conn.close()
        return False
    cur.execute("INSERT INTO clan_invites (clan_id, invited_user_id, invited_by) VALUES (%s, %s, %s)", (clan_id, invited_user_id, invited_by))
    conn.commit()
    cur.close(); conn.close()
    return True

def get_invites(user_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ci.*, c.name as clan_name, u.username as inviter_name
        FROM clan_invites ci JOIN clans c ON c.id = ci.clan_id JOIN users u ON u.user_id = ci.invited_by
        WHERE ci.invited_user_id = %s
    """, (user_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def accept_invite(invite_id, user_id):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM clan_invites WHERE id = %s AND invited_user_id = %s", (invite_id, user_id))
    invite = cur.fetchone()
    if invite:
        cur.execute("UPDATE users SET clan_id = %s, clan_role = 'member' WHERE user_id = %s", (invite['clan_id'], user_id))
        cur.execute("DELETE FROM clan_invites WHERE invited_user_id = %s", (user_id,))
        conn.commit()
        cur.close(); conn.close()
        return True
    cur.close(); conn.close()
    return False

def decline_invite(invite_id, user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM clan_invites WHERE id = %s AND invited_user_id = %s", (invite_id, user_id))
    conn.commit()
    cur.close(); conn.close()
