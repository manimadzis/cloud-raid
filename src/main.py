import asyncio
import signal
import sys

from loguru import logger

from cli import CLI, Parser
from config import Config


async def main():
    parser = Parser()
    args = parser.parse_args()

    logger.remove()
    logger.add(args.log, rotation="20 MB", enqueue=True)
    if args.debug:
        logger.add(sys.stderr, colorize=True, enqueue=True, format="{time} | {level} | {message}")

    config: Config = Config()

    if args.config_path:
        try:
            config = Config.load(args.config_path)
        except Exception as e:
            logger.exception(e)
            print("Invalid path to config")
    else:
        try:
            config = Config.load("config.toml")
        except Exception as e:
            logger.exception(e)
            config = Config()
            config.save("config.toml")

    cli = CLI(config, parser)

    async def sigint_handler(signal):
        await cli.close()
        cli.interrupt()

        tasks = asyncio.all_tasks()
        for task in tasks:
            task.cancel()

        loop.stop()

    #
    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame),
                                lambda: asyncio.create_task(sigint_handler(signame)))

    await cli.init()
    try:
        await cli.start()
    except Exception as e:
        logger.exception(e)
        print("Exception occurred")
    finally:
        await cli.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(e)
