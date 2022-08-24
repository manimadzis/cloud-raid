import asyncio
from argparse import ArgumentParser

from cli import CLI, Parser
from config import Config
from network.balancer import Balancer
from storage.block_repo import BlockRepo


def init_parser(parser: ArgumentParser):
    parser.add_argument('--config', dest='config_path', default="config.toml")


async def main():
    parser = Parser()
    args = parser.parse_args()

    config = Config.load(args.config_path)
    if not config:
        raise Exception("Cannot load config")

    cli = CLI(config, parser)
    await cli.start()


if __name__ == '__main__':
    asyncio.run(main())
