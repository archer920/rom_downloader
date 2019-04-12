import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from typing import AsyncIterable, AsyncIterator, Any, TypeVar

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver

logger = logging.getLogger(__name__)


class RomDriver:
    with open('jquery-3.3.1.min.js') as f:
        jq = f.read()

    def __init__(self) -> None:
        self.driver: WebDriver = None

    async def open(self) -> None:
        options = Options()
        options.headless = True
        self.driver = webdriver.Firefox(options=options)

    async def close(self) -> None:
        if self.driver:
            self.driver.close()

    async def navigate(self, url: str) -> None:
        self.driver.get(url)

    async def run_jq(self) -> None:
        self.driver.execute_script(RomDriver.jq)

    async def run_script(self, script: str) -> Any:
        return self.driver.execute_script(script)


ConsolePlatform = TypeVar('ConsolePlatform', bound='Platform')


@dataclass
class Platform:
    name: str
    total_roms: int
    url: str
    id: int = None
    page_num: int = None

    @staticmethod
    async def create_table(conn: sqlite3.Connection) -> None:
        sql = '''
            CREATE TABLE IF NOT EXISTS "Platform" (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "name"	TEXT,
	            "total_roms"	INTEGER NOT NULL,
	            "url"	TEXT,
	            "page_num" INTEGER
	        );
        '''
        c = conn.cursor()
        c.execute(sql)
        conn.commit()

    @staticmethod
    async def fetch_all(conn: sqlite3.Connection) -> AsyncIterator:
        sql = '''
            SELECT id, name, total_roms, url, page_num FROM PLATFORM ORDER BY name
        '''
        c = conn.cursor()
        c.execute(sql)
        for row in c.fetchall():
            yield Platform(id=row[0], name=row[1], total_roms=row[2], url=row[3], page_num=row[4])

    @staticmethod
    async def exists(name: str, conn: sqlite3.Connection) -> bool:
        sql = '''
            SELECT COUNT(*) FROM Platform WHERE name = ?
        '''
        c = conn.cursor()
        c.execute(sql, [name])
        rs = c.fetchone()
        return rs[0] > 0

    async def save(self, conn: sqlite3.Connection) -> ConsolePlatform:
        sql = '''
            INSERT INTO PLATFORM ('name', 'total_roms', 'url', 'page_num') VALUES (?, ?, ?, ?)
        '''
        c = conn.cursor()
        c.execute(sql, (self.name, self.total_roms, self.url, None))
        conn.commit()
        print('Inserted', self)

        sql = '''
            select id, name, total_roms, url, page_num from PLATFORM where name = ?
        '''
        c.execute(sql, [self.name])
        rs = c.fetchone()
        return Platform(id=rs[0], name=rs[1], total_roms=rs[2], url=rs[3], page_num=rs[4])

    async def update_page_num(self, conn: sqlite3.Connection) -> None:
        sql = '''
            UPDATE PLATFORM SET page_num=? where id=?
        '''
        c = conn.cursor()
        c.execute(sql, (self.page_num, self.id))
        conn.commit()


@dataclass
class Rom:
    title: str
    platform: str
    description_page_url: str
    landing_page: str
    download_url: str
    downloaded: bool
    not_found: bool

    @staticmethod
    async def create_table(conn: sqlite3.Connection) -> None:
        sql = '''
            CREATE TABLE IF NOT EXISTS "Rom" (
	            "Title"	TEXT,
	            "Platform"	TEXT NOT NULL,
	            "Description_Page"	TEXT,
	            "Landing_Page"	TEXT,
	            "Download_Url"	TEXT,
	            "Downloaded"	INTEGER,
	            "Not_Found"	INTEGER,
	            FOREIGN KEY (Platform) REFERENCES Platform(Name)
            );
        '''
        c = conn.cursor()
        c.execute(sql)
        conn.commit()


async def open_db(name='roms.db') -> sqlite3.Connection:
    return sqlite3.connect(name)


# noinspection PyBroadException
async def scrape_platforms(conn: sqlite3.Connection, q: asyncio.Queue) -> None:
    js = '''
        const platforms = [];
        $('.table > tbody:nth-child(2)').find('tr').each(function(){
	        const name = $(this).find('a').text();
            const url = $(this).find('a').attr('href');
            const total_roms = $(this).find('td:nth-child(2)').text();
 
 	        platforms.push({'name': name, 'url': url, 'total_roms': total_roms});
        });
        return platforms
    '''
    driver = RomDriver()
    try:
        await driver.open()
        await driver.navigate('https://romsmania.cc/roms')
        await driver.run_jq()
        all_platforms = await driver.run_script(js)
        for p in all_platforms:
            exists = await Platform.exists(p['name'], conn)
            if not exists:
                platform = await Platform(name=p['name'], total_roms=p['total_roms'], url=p['url']).save(conn)
                await q.put(platform)
            else:
                print(f"{p['name']} already exists")
    except Exception:
        logger.exception('Failed to scrape platforms')
    finally:
        await driver.close()


async def main() -> None:
    platform_queue = asyncio.Queue()
    conn = await open_db()

    await Rom.create_table(conn)
    await Platform.create_table(conn)
    await scrape_platforms(conn, platform_queue)


if __name__ == '__main__':
    asyncio.run(main())
