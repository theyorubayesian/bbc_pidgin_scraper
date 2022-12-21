from typing import List

from bs4 import BeautifulSoup


def get_page_count(soup: BeautifulSoup) -> int:
    count = 1
    ul: List[BeautifulSoup] = soup.find_all("ul", attrs={"class": "bbc-f8df6t e19602dz4"})
    
    if ul:
        count = int(ul[0].find_all("li")[-1].text)
    
    return count


def clean_string(x: str) -> str:
    return (
        x.replace(" ", "_")
        .replace("\\", "")
        .replace("/", "")
        .replace("(", "")
        .replace(")", "")
    )


def _is_valid_url_sw(href: str) -> bool:
    href = href.replace("https://www.bbc.com", "")
     
    if href.startswith("/swahili/articles/"):
        return True
    elif href.startswith("/swahili/habari-") or href.startswith("/swahili/"):
        if not (
            href.startswith("/swahili/topics")
            or href.startswith("/swahili/michezo")
            or href.startswith("/swahili/bbc_swahili_radio")
            or href.startswith("/swahili/dira-tv")
            or href.startswith('/swahili/media')
            or href.startswith("/swahili/taasisi")
        ):
            if href[-1].isdigit():
                return True
    else:
        return False


is_valid_url_factory = {
    "sw": _is_valid_url_sw,

}