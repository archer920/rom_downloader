import asyncio
import logging
import sqlite3
import colorama
from dataclasses import dataclass
from typing import AsyncIterable, AsyncIterator, Any, TypeVar, List

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from colorama import Fore

colorama.init()
logger = logging.getLogger(__name__)

NUM_WORKERS = 8


async def print_color(color: int, name: str, message: str) -> None:
    msg = f'[{name}]: {message}'
    print(color + msg)


class RomDriver:
    with open('jquery-3.3.1.min.js') as f:
        jq = f.read()

    headless: bool = False
    lock: asyncio.Lock = None
    driver_instances = 0
    driver_queue: asyncio.Queue = None

    @staticmethod
    async def setup(lock: asyncio.Lock, queue: asyncio.Queue) -> None:
        RomDriver.lock = lock
        RomDriver.driver_queue = queue

    @staticmethod
    async def tear_down() -> None:
        while not RomDriver.driver_queue.empty():
            driver = await RomDriver.driver_queue.get()
            try:
                driver.close()
            except Exception:
                logger.exception('Failed to destroy web driver')

    def __init__(self) -> None:
        self.driver: WebDriver = None

    async def open(self) -> None:
        async with RomDriver.lock:
            if RomDriver.driver_instances < NUM_WORKERS:
                if RomDriver.headless:
                    options = Options()
                    options.headless = True
                    await RomDriver.driver_queue.put(webdriver.Firefox(options=options))
                else:
                    await RomDriver.driver_queue.put(webdriver.Firefox())
                RomDriver.driver_instances += 1
        self.driver = await RomDriver.driver_queue.get()

    async def close(self) -> None:
        await RomDriver.driver_queue.put(self.driver)

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
    max_pages: int = None
    color: int = Fore.BLUE

    @staticmethod
    async def create_table(conn: sqlite3.Connection) -> None:
        sql = '''
            CREATE TABLE IF NOT EXISTS "Platform" (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "name"	TEXT,
	            "total_roms"	INTEGER NOT NULL,
	            "url"	TEXT,
	            "page_num" INTEGER,
	            "max_pages" INTEGER
	        );
        '''
        c = conn.cursor()
        c.execute(sql)
        conn.commit()

    @staticmethod
    async def fetch_all(conn: sqlite3.Connection) -> List[ConsolePlatform]:
        sql = '''
            SELECT id, name, total_roms, url, page_num, max_pages FROM PLATFORM ORDER BY name
        '''
        c = conn.cursor()
        c.execute(sql)

        platform_list = []
        for row in c.fetchall():
            platform_list.append(
                Platform(id=row[0], name=row[1], total_roms=row[2], url=row[3], page_num=row[4], max_pages=row[5]))
        return platform_list

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
            INSERT INTO PLATFORM ('name', 'total_roms', 'url', 'page_num', 'max_pages') VALUES (?, ?, ?, ?, ?)
        '''
        c = conn.cursor()
        c.execute(sql, (self.name, self.total_roms, self.url, None, None))
        conn.commit()
        await print_color(self.color, self.__class__.__name__, 'Inserted: ' + str(self))

        sql = '''
            select id, name, total_roms, url, page_num, max_pages from PLATFORM where name = ?
        '''
        c.execute(sql, [self.name])
        rs = c.fetchone()
        return Platform(id=rs[0], name=rs[1], total_roms=rs[2], url=rs[3], page_num=rs[4], max_pages=rs[5])

    async def update_page_num(self, conn: sqlite3.Connection) -> None:
        sql = '''
            UPDATE PLATFORM SET page_num=? where id=?
        '''
        c = conn.cursor()
        c.execute(sql, (self.page_num, self.id))
        conn.commit()

    async def update_max_pages(self, conn: sqlite3.Connection) -> None:
        sql = '''
                    UPDATE PLATFORM SET max_pages=? where id=?
                '''
        c = conn.cursor()
        c.execute(sql, (self.max_pages, self.id))
        conn.commit()


NewRom = TypeVar('NewRom', bound='Rom')


@dataclass
class Rom:
    title: str
    platform: str
    description_page_url: str
    id: int = None
    landing_page: str = None
    download_url: str = None
    downloaded: bool = None
    not_found: bool = None
    color: int = Fore.YELLOW

    @staticmethod
    async def create_table(conn: sqlite3.Connection) -> None:
        sql = '''
            CREATE TABLE IF NOT EXISTS "Rom" (
                "ID"	INTEGER PRIMARY KEY AUTOINCREMENT,
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

    async def save(self, conn: sqlite3.Connection, color: int = None) -> None:
        sql = '''INSERT INTO ROM ('TITLE', 'PLATFORM', 'DESCRIPTION_PAGE', 'LANDING_PAGE', 'DOWNLOAD_URL', 
        'DOWNLOADED', 'NOT_FOUND') VALUES (?, ?, ?, ?, ?, ?, ?) '''
        c = conn.cursor()
        c.execute(sql, (
            self.title, self.platform, self.description_page_url, self.landing_page, self.download_url, self.downloaded,
            self.not_found))
        conn.commit()
        if not color:
            color = self.color
        await print_color(color, f'{self.__class__.__name__}-{self.platform}', f'Inserted {self}')

    @staticmethod
    async def count_by_platform(platform: Platform, conn: sqlite3.Connection) -> int:
        sql = '''
            select count(*) from rom where platform=?
        '''
        c = conn.cursor()
        c.execute(sql, [platform.name])
        return c.fetchone()[0]


async def open_db(name='roms.db') -> sqlite3.Connection:
    return sqlite3.connect(name)


# noinspection PyBroadException
async def scrape_platforms(conn: sqlite3.Connection) -> None:
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
            total_roms = int(p['total_roms'].replace(',', ''))
            if not exists:
                await Platform(name=p['name'], total_roms=total_roms, url=p['url']).save(conn)
            else:
                await print_color(Fore.CYAN, name='scrape_platforms', message=f"{p['name']} already exists")
    except Exception:
        logger.exception('Failed to scrape platforms')
    finally:
        await driver.close()


# noinspection PyBroadException
async def scrape_rom_title_description_url(conn: sqlite3.Connection, queue: asyncio.Queue, color: int) -> None:
    platform = await queue.get()

    if platform.max_pages:
        if platform.max_pages == platform.page_num:
            # We have already finished this platform
            queue.task_done()
            return

    driver = RomDriver()
    try:
        await driver.open()
        await driver.navigate(platform.url)
        if not platform.max_pages:
            platform.max_pages = await find_max_pages_for_platform(driver)
            platform.page_num = 0

            await platform.update_max_pages(conn)
            await platform.update_page_num(conn)

        cur_page = 1
        while cur_page < platform.page_num:
            advanced = await advance_platform_page(driver)
            if advanced:
                cur_page += 1

        while platform.page_num < platform.max_pages:
            await print_color(color, platform.name, f'On page {platform.page_num}')
            js = '''
                const rs = [];
                $('div.results > table').find('a').each(function(){
	                const title = $(this).text();
                    const href = $(this).attr('href');
  
                    rs.push({
  	                    'title': title,
                        'href': href
                    });
                });
                return rs;
                '''
            rs = await driver.run_script(js)
            for row in rs:
                rom = Rom(title=row['title'],
                          platform=platform.name,
                          description_page_url=row['href'])
                await rom.save(conn, color=color)
            await advance_platform_page(driver)
            platform.page_num += 1
            await platform.update_page_num(conn)

        record_count = await Rom.count_by_platform(platform, conn)
        if record_count != platform.total_roms:
            #  We would expect that the the number of records we create in the DB
            #  matches the total number of games that the site reports.
            await print_color(Fore.RED, 'FIXME', f'Total roms = {platform.total_roms}, scraped_roms = {record_count}')
        queue.task_done()
    except Exception:
        logger.exception(f'Exception while scraping platform: ${platform}')
    finally:
        await driver.close()


async def advance_platform_page(driver: RomDriver) -> bool:
    js = '''
            let advanced = false;
            $('.pagination__list').find('a').each(function(){
    	        const text = $(this).text().trim();
                if(text === 'Next'){
      	            $(this).trigger('click');
                    advanced = true;
                }
            });
            return advanced;
        '''
    val = await driver.run_script(js)
    await asyncio.sleep(1)
    return val


async def find_max_pages_for_platform(driver: RomDriver) -> int:
    js = '''
        let max_pages = 1;

        $('.pagination__list').find('li').each(function(){
	        const cur = parseInt($(this).text());
            if (!isNaN(cur)){
  	            if (cur > max_pages){
    	            max_pages = cur;
                }
            }
        });
        return max_pages;
    '''
    return await driver.run_script(js)


async def color_list(size: int) -> List[int]:
    colors_tup = (Fore.GREEN, Fore.YELLOW, Fore.BLUE, Fore.MAGENTA, Fore.CYAN, Fore.WHITE)
    color_index = 0
    colors_list = []
    for _ in range(size):
        colors_list.append(colors_tup[color_index])
        color_index += 1
        if color_index >= len(colors_tup):
            color_index = 0
    return colors_list


# noinspection PyBroadException
async def main() -> None:
    try:
        conn = await open_db()

        await RomDriver.setup(asyncio.Lock(), asyncio.Queue())
        await Rom.create_table(conn)
        await Platform.create_table(conn)
        await scrape_platforms(conn)

        platform_queue = asyncio.Queue()
        all_platforms = await Platform.fetch_all(conn)
        for pf in all_platforms:
            await platform_queue.put(pf)

        tasks = []
        console_colors = await color_list(platform_queue.qsize())
        for i in range(platform_queue.qsize()):
            tasks.append(asyncio.create_task(scrape_rom_title_description_url(conn=conn, queue=platform_queue, color=console_colors.pop(0))))
        await platform_queue.join()
        for t in tasks:
            t.cancel()

    except Exception:
        logger.exception('Exception while running the program')
    finally:
        await RomDriver.tear_down()


if __name__ == '__main__':
    asyncio.run(main())
