import argparse
import asyncio
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import AsyncIterable, Any, TypeVar, List

import aiohttp as aiohttp
import colorama
from colorama import Fore
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver

colorama.init()
logger = logging.getLogger(__name__)

NUM_WORKERS = 8


async def print_color(color: int, name: str, message: str) -> None:
    msg = f'[{name}]: {message}'
    print(color + msg)


class RomDriver:
    with open('jquery-3.3.1.min.js') as f:
        jq = f.read()

    headless: bool = True
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


class SingleRomDriver:
    with open('jquery-3.3.1.min.js') as f:
        jq = f.read()

    def __init__(self) -> None:
        super().__init__()
        self.driver: WebDriver = None

    async def open(self, headless=True) -> None:
        if headless:
            options = Options()
            options.headless = True
            self.driver = webdriver.Firefox(options=options)
        else:
            self.driver = webdriver.Firefox()

    async def close(self) -> None:
        self.driver.close()

    async def navigate(self, url: str) -> None:
        self.driver.get(url)

    async def run_jq(self) -> None:
        self.driver.execute_script(SingleRomDriver.jq)

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
    async def fetch_by_name(conn: sqlite3.Connection, platform: str) -> ConsolePlatform:
        sql = '''
            SELECT id, name, total_roms, url, page_num, max_pages 
            FROM PLATFORM
            WHERE name = ?
        '''
        c = conn.cursor()
        c.execute(sql, [platform])
        rs = c.fetchone()
        return Platform(
            id=rs[0],
            name=rs[1],
            total_roms=rs[2],
            url=rs[3],
            page_num=rs[4],
            max_pages=rs[5]
        )

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

    async def save_landing_page(self, conn: sqlite3.Connection, color: int) -> None:
        sql = '''
            UPDATE ROM
            SET LANDING_PAGE = ?
            WHERE ID = ?
        '''
        c = conn.cursor()
        c.execute(sql, (self.landing_page, self.id))
        conn.commit()
        if not color:
            color = self.color
        await print_color(color,
                          f'{self.__class__.__name__}-{self.platform}',
                          f'Saved landing page = {self.landing_page}')

    @staticmethod
    async def count_by_platform(platform: Platform, conn: sqlite3.Connection) -> int:
        sql = '''
            select count(*) from rom where platform=?
        '''
        c = conn.cursor()
        c.execute(sql, [platform.name])
        return c.fetchone()[0]

    @staticmethod
    async def fetch_all_by_where_description_is_not_null_and_landing_page_is_null(conn: sqlite3.Connection,
                                                                                  platform: str) -> \
            AsyncIterable[NewRom]:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT ID, TITLE, PLATFORM, DESCRIPTION_PAGE, LANDING_PAGE, DOWNLOAD_URL, DOWNLOADED, NOT_FOUND
                FROM ROM
                WHERE DESCRIPTION_PAGE IS NOT NULL AND LANDING_PAGE IS NULL
                LIMIT 25
            '''
            c.execute(sql)
        else:
            sql = '''
                    SELECT ID, TITLE, PLATFORM, DESCRIPTION_PAGE, LANDING_PAGE, DOWNLOAD_URL, DOWNLOADED, NOT_FOUND
                    FROM ROM
                    WHERE DESCRIPTION_PAGE IS NOT NULL AND LANDING_PAGE IS NULL AND PLATFORM = ?
                    LIMIT 25
            '''
            c.execute(sql, [platform])

        for row in c.fetchall():
            yield Rom(id=row[0],
                      title=row[1],
                      platform=row[2],
                      description_page_url=row[3],
                      landing_page=row[4],
                      download_url=row[5],
                      downloaded=row[6],
                      not_found=row[7])

    @staticmethod
    async def count_by_description_and_landing_page(conn: sqlite3.Connection, platform: str) -> int:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DESCRIPTION_PAGE IS NOT NULL AND LANDING_PAGE IS NOT NULL
            '''
            c.execute(sql)
        else:
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DESCRIPTION_PAGE IS NOT NULL AND LANDING_PAGE IS NOT NULL AND PLATFORM = ?
            '''
            c.execute(sql, [platform])
        return c.fetchone()[0]

    @staticmethod
    async def count_all(conn: sqlite3.Connection, platform: str) -> int:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT COUNT(*)
                FROM ROM
            '''
            c.execute(sql)
        else:
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE PLATFORM = ?
            '''
            c.execute(sql, [platform])
        return c.fetchone()[0]

    @staticmethod
    async def has_download_urls(conn: sqlite3.Connection, platform: str) -> bool:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE LANDING_PAGE IS NOT NULL AND DOWNLOAD_URL IS NULL
            '''
            c.execute(sql)
            return c.fetchone()[0] > 0
        else:
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE LANDING_PAGE IS NOT NULL AND DOWNLOAD_URL IS NULL AND PLATFORM = ?
            '''
            c.execute(sql, [platform])
            return c.fetchone()[0] > 0

    @staticmethod
    async def fetch_all_by_landing_page_null_download_url(conn: sqlite3.Connection, platform: str) -> AsyncIterable[
        NewRom]:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT ID, TITLE, PLATFORM, DESCRIPTION_PAGE, LANDING_PAGE, DOWNLOAD_URL, DOWNLOADED, NOT_FOUND
                FROM ROM
                WHERE LANDING_PAGE IS NOT NULL AND DOWNLOAD_URL IS NULL
                LIMIT 25
                
            '''
            c.execute(sql)
        else:
            sql = '''
                SELECT ID, TITLE, PLATFORM, DESCRIPTION_PAGE, LANDING_PAGE, DOWNLOAD_URL, DOWNLOADED, NOT_FOUND
                FROM ROM
                WHERE LANDING_PAGE IS NOT NULL AND DOWNLOAD_URL IS NULL AND PLATFORM = ?
                LIMIT 25
            '''
            c.execute(sql, [platform])

        for row in c.fetchall():
            yield Rom(id=row[0],
                      title=row[1],
                      platform=row[2],
                      description_page_url=row[3],
                      landing_page=row[4],
                      download_url=row[5],
                      downloaded=row[6],
                      not_found=row[7])

    async def save_download_url(self, conn: sqlite3.Connection, color: int):
        sql = '''
            UPDATE ROM
            SET DOWNLOAD_URL = ?
            WHERE ID = ?
        '''
        c = conn.cursor()
        c.execute(sql, (self.download_url, self.id))
        conn.commit()
        if not color:
            color = self.color
        await print_color(color,
                          f'{self.__class__.__name__}-{self.platform}',
                          f'Saved download_url = {self.download_url}')

    @staticmethod
    async def count_by_landing_page_null_download_url(conn: sqlite3.Connection, platform: str) -> int:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE LANDING_PAGE IS NOT NULL AND DOWNLOAD_URL IS NOT NULL
            '''
            c.execute(sql)
        else:
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE LANDING_PAGE IS NOT NULL AND DOWNLOAD_URL IS NOT NULL AND PLATFORM = ?
            '''
            c.execute(sql, [platform])
        return c.fetchone()[0]

    @staticmethod
    async def has_landing_pages_to_scrape(conn: sqlite3.Connection, platform: str) -> bool:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DESCRIPTION_PAGE IS NOT NULL AND LANDING_PAGE IS NULL
            '''
            c.execute(sql)
        else:
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DESCRIPTION_PAGE IS NOT NULL AND LANDING_PAGE IS NULL AND PLATFORM = ?
            '''
            c.execute(sql, [platform])
        return c.fetchone()[0] > 0

    @staticmethod
    async def has_roms_to_download(conn: sqlite3.Connection, platform: str) -> bool:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DOWNLOAD_URL IS NOT NULL AND DOWNLOADED IS NULL AND NOT_FOUND IS NULL
            '''
            c.execute(sql)
        else:
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DOWNLOAD_URL IS NOT NULL AND DOWNLOADED IS NULL AND NOT_FOUND IS NULL AND PLATFORM = ?
            '''
            c.execute(sql, [platform])
        return c.fetchone()[0] > 0

    @staticmethod
    async def fetch_roms_ready_to_download(conn: sqlite3.Connection, platform: str) -> AsyncIterable[NewRom]:
        c = conn.cursor()
        if platform == 'ALL':
            sql = f'''
                SELECT ID, TITLE, PLATFORM, DESCRIPTION_PAGE, LANDING_PAGE, DOWNLOAD_URL, DOWNLOADED, NOT_FOUND
                FROM ROM
                WHERE DOWNLOAD_URL IS NOT NULL AND DOWNLOADED IS NULL AND NOT_FOUND IS NULL
                LIMIT {NUM_WORKERS}
            '''
            c.execute(sql)
        else:
            sql = f'''
                SELECT ID, TITLE, PLATFORM, DESCRIPTION_PAGE, LANDING_PAGE, DOWNLOAD_URL, DOWNLOADED, NOT_FOUND
                FROM ROM
                WHERE DOWNLOAD_URL IS NOT NULL AND DOWNLOADED IS NULL AND NOT_FOUND IS NULL AND PLATFORM = ?
                LIMIT {NUM_WORKERS}
            '''
            c.execute(sql, [platform])
        for row in c.fetchall():
            yield Rom(id=row[0],
                      title=row[1],
                      platform=row[2],
                      description_page_url=row[3],
                      landing_page=row[4],
                      download_url=row[5],
                      downloaded=row[6],
                      not_found=row[7])

    async def mark_downloaded(self, conn: sqlite3.Connection, color: int) -> None:
        sql = '''
            UPDATE ROM
            SET DOWNLOADED = ?
            WHERE ID = ?
        '''
        c = conn.cursor()
        c.execute(sql, (self.downloaded, self.id))
        conn.commit()
        if not color:
            color = self.color
        await print_color(color,
                          f'{self.__class__.__name__}-{self.platform}',
                          f'Downloaded = {self.title}')

    @staticmethod
    async def count_roms_to_download(conn: sqlite3.Connection, platform: str) -> int:
        c = conn.cursor()
        if platform == 'ALL':
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DOWNLOAD_URL IS NOT NULL AND DOWNLOADED IS NOT NULL AND NOT_FOUND IS NULL
            '''
            c.execute(sql)
        else:
            sql = '''
                SELECT COUNT(*)
                FROM ROM
                WHERE DOWNLOAD_URL IS NOT NULL AND DOWNLOADED IS NOT NULL AND NOT_FOUND IS NULL AND PLATFORM = ?
            '''
            c.execute(sql, [platform])
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


async def scrape_rom_titles(conn: sqlite3.Connection, platform: str) -> None:
    platform_queue = asyncio.Queue()
    if platform == 'ALL':
        all_platforms = await Platform.fetch_all(conn)
    else:
        pf = await Platform.fetch_by_name(conn, platform)
        all_platforms = [pf]
    for pf in all_platforms:
        await platform_queue.put(pf)
    tasks = []
    console_colors = await color_list(platform_queue.qsize())
    for i in range(platform_queue.qsize()):
        tasks.append(
            asyncio.create_task(
                scrape_rom_title_description_url(conn=conn, queue=platform_queue, color=console_colors.pop(0))))
    await platform_queue.join()
    for t in tasks:
        t.cancel()


async def scrape_landing_page(conn: sqlite3.Connection, queue: asyncio.Queue, color: int) -> None:
    driver = RomDriver()
    try:
        rom = await queue.get()
        await driver.open()
        await driver.navigate(rom.description_page_url)
        js = '''
            return $('#download_link').attr('href');
        '''
        href = await driver.run_script(js)
        rom.landing_page = href
        await rom.save_landing_page(conn, color)
        queue.task_done()
    except Exception as e:
        logger.exception('Failed to scrape landing page', e)
    finally:
        await driver.close()


async def scrape_landing_page_progress(conn: sqlite3.Connection, platform: str) -> None:
    roms = await Rom.count_by_description_and_landing_page(conn, platform)
    all_roms = await Rom.count_all(conn, platform)
    per = (float(roms) / float(all_roms)) * 100
    await print_color(Fore.LIGHTYELLOW_EX, 'Landing Page Progress', '{0:.2f}% Completed'.format(per))


async def scrape_landing_pages(conn: sqlite3.Connection, platform: str) -> None:
    has_work = await Rom.has_landing_pages_to_scrape(conn, platform)
    while has_work:
        queue = asyncio.Queue()
        async for rom in Rom.fetch_all_by_where_description_is_not_null_and_landing_page_is_null(conn, platform):
            await queue.put(rom)
        console_colors = await color_list(queue.qsize())
        tasks = []
        for i in range(queue.qsize()):
            tasks.append(
                asyncio.create_task(
                    scrape_landing_page(conn, queue, console_colors.pop(0))))
            if i % 20 == 0:
                tasks.append(asyncio.create_task(scrape_landing_page_progress(conn, platform)))
        await queue.join()
        for t in tasks:
            t.cancel()
        has_work = await Rom.has_landing_pages_to_scrape(conn, platform)
        await print_color(Fore.WHITE, 'scrape_landing_pages', 'Gathering more work...')


async def scrape_download_url(conn: sqlite3.Connection, queue: asyncio.Queue, color: int,
                              sem: asyncio.Semaphore) -> None:
    rom: Rom = await queue.get()
    async with sem:
        try:
            driver = SingleRomDriver()
            await driver.open()
            await driver.navigate(rom.landing_page)
            js = '''
                return $('.wait__link').attr('href');
            '''
            rom.download_url = await driver.run_script(js)
        except Exception as e:
            logger.exception('Failed to scrape download page', e)
        finally:
            await driver.close()
    await rom.save_download_url(conn, color)
    queue.task_done()


async def scrape_download_url_progress(conn: sqlite3.Connection, platform: str) -> None:
    roms = await Rom.count_by_landing_page_null_download_url(conn, platform)
    all_roms = await Rom.count_all(conn, platform)
    per = (float(roms) / float(all_roms)) * 100
    await print_color(Fore.LIGHTYELLOW_EX, 'Download URL Progress', '{0:.2f}% Completed'.format(per))


async def scrape_download_urls(conn: sqlite3.Connection, platform: str) -> None:
    has_work = await Rom.has_download_urls(conn, platform)
    while has_work:
        queue = asyncio.Queue()
        sem = asyncio.Semaphore(NUM_WORKERS)

        async for rom in Rom.fetch_all_by_landing_page_null_download_url(conn, platform):
            await queue.put(rom)
        console_colors = await color_list(queue.qsize())

        tasks = []
        for i in range(queue.qsize()):
            tasks.append(
                asyncio.create_task(
                    scrape_download_url(conn, queue, console_colors.pop(0), sem)
                )
            )
            if i % 20 == 0:
                tasks.append(
                    asyncio.create_task(
                        scrape_download_url_progress(conn, platform)
                    )
                )
        await queue.join()
        for t in tasks:
            t.cancel()
        has_work = await Rom.has_download_urls(conn, platform)
        await print_color(Fore.WHITE, 'scrape_download_urls', 'Gathering more work...')


async def download_rom_progress(conn: sqlite3.Connection, platform: str) -> None:
    roms = await Rom.count_roms_to_download(conn, platform)
    all_roms = await Rom.count_all(conn, platform)
    per = (float(roms) / float(all_roms)) * 100
    await print_color(Fore.LIGHTYELLOW_EX, 'Download Rom Progress', '{0:.2f}% Completed'.format(per))


async def download_roms(conn: sqlite3.Connection, platform: str) -> None:
    has_work = await Rom.has_roms_to_download(conn, platform)
    while has_work:
        queue = asyncio.Queue()
        sem = asyncio.Semaphore(NUM_WORKERS)

        async for rom in Rom.fetch_roms_ready_to_download(conn, platform):
            await queue.put(rom)
        console_colors = await color_list(queue.qsize())

        tasks = []
        for i in range(queue.qsize()):
            tasks.append(
                asyncio.create_task(
                    download_rom(conn, queue, console_colors.pop(0))
                )
            )

        tasks.append(
            asyncio.create_task(
                download_rom_progress(conn, platform)
            )
        )
        await queue.join()
        for t in tasks:
            t.cancel()

        await print_color(Fore.WHITE, 'download_roms', 'Gathering more work...')
        has_work = await Rom.has_roms_to_download(conn, platform)


async def download_rom(conn: sqlite3.Connection, queue: asyncio.Queue, color: int) -> None:
    rom: Rom = await queue.get()
    download_folder = await make_save_dir()
    platform_folder = await make_platform_dir(download_folder, rom)
    f_extension = rom.download_url.split('.')[-1]
    f_path = os.path.join(os.path.sep, platform_folder, rom.title + '.' + f_extension)

    if os.path.exists(f_path):
        f_path = f_path + '.' + str(int(round(time.time() * 1000)))

    await perform_rom_download(rom, f_path)
    await rom.mark_downloaded(conn, color)
    queue.task_done()


async def perform_rom_download(rom: Rom, f_path: str):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(rom.download_url) as response:
                file_bytes = await response.read()
                with open(f_path, 'wb') as f:
                    f.write(file_bytes)
                    rom.downloaded = True
    except Exception as e:
        logger.exception(f'Failed to download {rom.title}', e)


async def make_save_dir() -> str:
    rom_dir = os.path.join(os.path.sep, os.getcwd(), 'downloads')
    if not os.path.exists(rom_dir):
        os.mkdir(rom_dir)

    return rom_dir


async def make_platform_dir(rom_dir: str, rom: Rom) -> str:
    platform_dir = os.path.join(os.path.sep, rom_dir, rom.platform.replace(' ', '_'))
    if not os.path.exists(platform_dir):
        os.mkdir(platform_dir)

    return platform_dir


async def main(platform: str) -> None:
    try:
        conn = await open_db()

        await RomDriver.setup(asyncio.Lock(), asyncio.Queue())
        await Rom.create_table(conn)
        await Platform.create_table(conn)
        await scrape_platforms(conn)

        can_proceed = True
        if platform != 'ALL':
            can_proceed = await Platform.exists(platform, conn)

        if can_proceed:
            await scrape_rom_titles(conn, platform)
            await scrape_landing_pages(conn, platform)
            await RomDriver.tear_down()

            await scrape_download_urls(conn, platform)
            await download_roms(conn, platform)
        else:
            logger.info(f'Unable to continue because {platform} is not found! Exiting...')

    except Exception as e:
        logger.exception('Exception while running the program', e)
    finally:
        await RomDriver.tear_down()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Roms from https://romsmania.cc/roms')
    parser.add_argument('--platform',
                        metavar='platform',
                        type=str,
                        help='Specify specific game console (i.e. NES, Sega)',
                        default='ALL')
    args = parser.parse_args()
    asyncio.run(main(args.platform))
