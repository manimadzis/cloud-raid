import asyncio
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

    config = Config.load(args.config_path)
    if not config:
        raise Exception("Cannot load config")

    cli = CLI(config, parser)
    await cli.init()
    await cli.start()


if __name__ == '__main__':
    asyncio.run(main())