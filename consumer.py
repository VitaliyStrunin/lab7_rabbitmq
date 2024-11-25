import asyncio
import os
import aiohttp
import aio_pika
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
TIMEOUT = int(os.getenv("TIMEOUT", 10))

async def process_message(message, session, channel):
    async with message.process():
        url = message.body.decode()
        print(f"Processing message: {url}")
        links = await extract_links(url, session)
        for link in links:
            await channel.default_exchange.publish(
                aio_pika.Message(body=link.encode()),
                routing_key="links",
            )
            print(f"Sent link: {link}")

async def extract_links(url, session):
    links = []
    try:
        async with session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            title = soup.title.string if soup.title else "No title"
            print(f"Processing page: {title} ({url})")

            for tag in soup.find_all(["a", "img", "video", "audio", "source"]):
                if tag.name == "a" and tag.get("href"):
                    link = tag["href"]
                elif tag.get("src"):
                    link = tag["src"]
                else:
                    continue

                full_url = urljoin(url, link)
                if urlparse(full_url).netloc == urlparse(url).netloc:
                    links.append(full_url)
                    tag_content = tag.string.strip() if tag.string else "No content"
                    print(f"Found link: {tag_content} ({full_url})")
    except Exception as e:
        print(f"Error processing {url}: {e}")
    return links

async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("links", durable=True)

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    message = await queue.get(timeout=TIMEOUT)
                    await process_message(message, session, channel)
                except asyncio.TimeoutError:
                    print("Queue is empty. Exiting...")
                    break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Program interrupted. Exiting...")
