from typing import Tuple

import aiohttp


async def get_disk_size(token: str, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """
    Return used space and total space in bytes

    """
    url = "https://cloud-api.yandex.net/v1/disk"

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'OAuth {token}'
    }

    params = {
        'fields': 'total_space,used_space'
    }

    async with session.get(url, params=params, headers=headers) as resp:
        if resp.status != 200:
            return 0, 0
        else:
            json_resp = await resp.json()

    return json_resp.get('used_space', 0), json_resp.get('total_space', 0)
