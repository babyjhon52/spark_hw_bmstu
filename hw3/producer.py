from telethon import TelegramClient, events
import asyncio
import json
import re
from confluent_kafka import Producer
from collections import Counter
from datetime import datetime
from telethon.tl.functions.channels import JoinChannelRequest

api_id = '_'
api_hash = '_'
GROUP_NAMES = ['@ufarabota_chat']

client = TelegramClient('session', api_id, api_hash)
producer = Producer(
    {'bootstrap.servers': 'localhost:9092', 'compression.type': 'gzip'})

stop_words = set(['в', 'а', 'с', 'и', 'но', 'я', 'ты',
                 'вы', 'он', 'мы', 'не', 'бы', 'же'])
RU_VOWELS = set('аеёиоуыэюя')
words = Counter()


def clean_word(word):
    word = re.sub(r'[^а-яА-ЯёЁ]', '', word)
    if not word or len(word) < 2:
        return ''
    word_core = ''.join(ch for ch in word if ch.lower() not in RU_VOWELS)
    if word_core.endswith('й'):
        word_core = word_core[:-1]
    return word_core


def extract_proper_nouns(text, stop_words):
    tokens = re.findall(r'\b[А-ЯЁ][а-яё]+\b', text)[1:]
    result = []
    for token in tokens:
        if token.lower() in stop_words:
            continue
        cleaned = clean_word(token)
        if cleaned:
            result.append(cleaned)
    return result


def delivery_report(err, msg):
    if err:
        print(f'Ошибка: {err}')
    else:
        print(f'Отправлено в partition {msg.partition()}')


async def save_and_reset_stats_periodically():
    while True:
        await asyncio.sleep(60)
        now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        if not words:
            continue

        data = {'timestamp': now, 'stats': dict(words)}

        producer.produce(
            'raw_messages',
            value=json.dumps(data, ensure_ascii=False).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
        producer.flush()
        words.clear()


@client.on(events.NewMessage(chats=GROUP_NAMES))
async def handler(event):
    if event.message.text:
        names = extract_proper_nouns(event.message.text, stop_words)
        words.update(names)


async def main():
    await client.start()
    for group_id in GROUP_NAMES:
        try:
            entity = await client.get_entity(group_id)
            print(f'Подключен к {entity.title}')
        except Exception as e:
            print(f'Ошибка {group_id}: {e}')
            return

    await client(JoinChannelRequest(entity))

    stats_task = asyncio.create_task(save_and_reset_stats_periodically())
    try:
        await client.run_until_disconnected()
    finally:
        stats_task.cancel()
        await client.disconnect()
        producer.flush()

if __name__ == '__main__':
    asyncio.run(main())
