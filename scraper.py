import argparse
import csv
import glob
import itertools
import json
import logging
import multiprocessing
import os
import time
from datetime import datetime
from functools import partial
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

logging.root.setLevel(logging.INFO)

CONFIG = yaml.load(open("config.yml"), Loader=yaml.FullLoader)
ALL_CATEGORIES = CONFIG["CATEGORY_URLS"]
OLDEST_ARTICLE_DATE = datetime.strptime(CONFIG["OLDEST_ARTICLE_DATE"], '%Y-%m-%d')


def get_parser() -> argparse.ArgumentParser:
    """
    parse command line arguments

    returns:
        parser - ArgumentParser object
    """
    parser = argparse.ArgumentParser(description="BBC Pidgin Scraper")
    parser.add_argument(
        "--output_file_name", 
        type=str, 
        default="bbc_pidgin_corpus.csv", 
        help="Name of output file",
        )
    parser.add_argument(
        "--no_of_articles", 
        type=int, 
        default=-1, 
        help="Number of articles to be scraped from the BBC pidgin website"
             "If -1, we scrape all articles we find",
        )
    parser.add_argument(
        "--categories", 
        type = str, 
        default= "all", 
        help= "Specify what news categories to scrape from." 
              "Multiple news categories should be separated by a comma. eg. 'africa,world,sport'",
        )
    
    parser.add_argument(
        "--time_delay", 
        type = bool, 
        default= True, 
        help= "Specify time delay after every url request",
        )
    
    parser.add_argument(
        "--spread",
        action="store_true",
        help="""Spread `no_of_articles` evenly across categories. If `most_popular` in categories, 
        all its articles are collected and the remainder is spread across other categories"""
    )

    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove sub-topic TSV files created after combining them into final corpora"
    )
    
    return parser


def get_page_soup(url:str) -> BeautifulSoup:
    """
    Makes a request to a url and creates a beautiful soup oject from the response html

    input:
        :param url: input page url
    returns:
        - page_soup: beautiful soup oject from the response html
    """

    response = requests.get(url)
    page_html = response.text
    page_soup = BeautifulSoup(page_html, "html.parser")

    return page_soup


def get_page_count(page_soup: BeautifulSoup) -> int:
    pagination_list = page_soup.find_all(
        "ul", attrs={"class": CONFIG["PAGINATION_LIST_CLASS"]}
    )
    if pagination_list:
        total_page_count = int(pagination_list[0].find_all("li")[-1].text)
        return total_page_count
    else:
        pagination_list = page_soup.find_all(
            "span", attrs={"class": CONFIG["ARTICLE_COUNT_SPAN"]}
            )
    
    if pagination_list:
        total_page_count = int(pagination_list[0].text)
        return total_page_count

    return 1


def get_urls(
    category_url:str, 
    category:str, 
    time_delay:bool, 
    articles_per_category: Optional[int] = None
    ) -> List[str]:
    """
    Obtains all the article urls from the category url it takes in

    input:
        :param categpry_url: category url
        :param category: category name
    returns:
        - category_urls: list of all valid article urls on all the category pages
    """
    page_soup = get_page_soup(category_url)
    category_urls = get_valid_urls(page_soup)
    logging.info(f"{len(category_urls)} urls in page 1 gotten for {category}")
    # get total number of pages for given category
    # article_count_span = page_soup.find_all(
    #     "span", attrs={"class": CONFIG["ARTICLE_COUNT_SPAN"]}
    #     )
    # pagination_list = page_soup.find_all(
    #     "ul", attrs={"class": CONFIG["PAGINATION_LIST_CLASS"]}
    # ) or page_soup.find_all

    # if there are multiple pages, get valid urls from each page
    # else just get the articles on the first page
    total_page_count = get_page_count(page_soup)
    logging.info(f"{total_page_count} page(s) found for {category}")

    if total_page_count > 1:
        # total_article_count = int(article_count_span[0].text)
        # total_article_count = int(pagination_list[0].find_all("li")[-1].text)
    # page_soup.find_all(
    #     "span", attrs={"class": CONFIG["ARTICLE_COUNT_SPAN"]}
    #     )
        if articles_per_category > 0 and len(category_urls) >= articles_per_category:
            return category_urls

        for count in range(1, total_page_count):
            page_soup = get_page_soup(category_url + f"?page={count+1}")
            page_urls = get_valid_urls(page_soup)
            logging.info(f"{len(page_urls)} urls in page {count+1} gotten for {category}")
            category_urls+=page_urls
            
            if articles_per_category > 0 and len(category_urls) >= articles_per_category:
                break

            if time_delay: 
                time.sleep(10)

    return category_urls


def get_valid_urls(category_page:BeautifulSoup) -> List[str]:
    """
    Gets all valid urls from a category page

    input:
        :param: url: category_page
    returns:
        - valid_urls: list of all valid article urls on a given category page
    """
    all_urls = category_page.findAll("a")
    valid_article_urls = []
    for url in all_urls:
        href: str = url.get("href")
        # from a look at BBC pidgin's urls, they always begin with the following strings. 
        # so we obtain valid article urls using these strings
        # if (
        #     href.startswith("/pidgin/tori") or href.startswith("/pidgin/world") or href.startswith("/pidgin/sport")
        #     ) and href[-1].isdigit():
            # story_url = "https://www.bbc.com" + href
        try:
            _, stub = list(filter(None, href.split("/")))
        except Exception as err:
            continue
        
        if stub.isdigit() or (stub.split("-")[0] in CONFIG["VALID_ARTICLE_URL_STUBS"] and stub[-1].isdigit()):
        # if stub.startswith("tori") or \
        #     stub.startswith("world") or \
        #         stub.startswith("media") or \
        #             stub.startswith("sport") and \
        #                 stub[-1].isdigit():
            story_url = "https://www.bbc.com" + href # if href.startswith("/pidgin") else href
            # print(story_url)
            # if "live" in story_url.split("/"):
            #     continue

            valid_article_urls.append(story_url)

    return list(set(valid_article_urls))


def get_topics(homepage: str, known_topic_urls: List[str]) -> Dict[str, str]:
    """
    Meant to be used with the homepage to recover all sub-topics available
    """
    page_soup = get_page_soup(homepage)
    article_urls = get_valid_urls(page_soup)
    topics = {}

    for url in article_urls:
        url_soup = get_page_soup(url)
        topic_elements = url_soup.find_all("li", attrs={"class": CONFIG["TOPIC_LIST_CLASS"]}) or []
        for topic in topic_elements:
            topic_url = "https://www.bbc.com" + topic.find("a").get("href")
            if topic_url not in known_topic_urls:
                topic_name = "_".join(topic.text.split()).upper().replace("/", "_").replace("\\", "_")
                topics[topic_name] = topic_url
    return topics


def get_headline(page_soup: BeautifulSoup) -> str:
    for cls in CONFIG["HEADLINE_SPAN_CLASS_A"]:
        headline_elem = page_soup.find(
            "h1", attrs={"class": cls}
        )
        if headline_elem:
            break
    
    if not headline_elem:
        for cls in CONFIG["HEADLINE_SPAN_CLASS_B"]:
            headline_elem = page_soup.find(
                "strong", attrs={"class": CONFIG["HEADLINE_SPAN_CLASS_B"]}
            )
            if headline_elem:
                break
    
    return headline_elem.text.strip() if headline_elem else ""


def get_article_data(article_url:str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Obtains paragraphs texts and headlines input url article

    input:
        :param article_url: category_page
    returns:
        - headline: headline of url article 
        - story_text: text of url article
        - article_url: input article url
    """
    page_soup = get_page_soup(article_url)
    article_date = page_soup.find("time", attrs={"class": CONFIG["ARTICLE_DATE_CLASS"]})

    if article_date:
        article_date = article_date.get("datetime")
        article_date = datetime.strptime(article_date, '%Y-%m-%d')
        if article_date <= OLDEST_ARTICLE_DATE:
            return ("","",article_url)
    
    headline = get_headline(page_soup)
    # headline = page_soup.find(
    #     "h1", attrs={"class": CONFIG["HEADLINE_SPAN_CLASS_A"]}
    #     )
    # # by inspection, if the headline is not in the class above, it should be in the one below
    # # TODO: Investigate if this is still necessary
    # if not headline:
    #     headline = page_soup.find(
    #         "strong", attrs={"class": CONFIG["HEADLINE_SPAN_CLASS_B"]}
    #         )
    
    # if headline:
    #     headline = headline.text.strip()
    
    story_text = " "
    story_div = page_soup.find_all(
        "div", attrs={"class": CONFIG["STORY_DIV_CLASS"]}
        )
    if story_div:
        all_paragraphs = [div.findAll("p", recursive=False) for div in story_div]
        all_paragraphs = list(itertools.chain(*all_paragraphs))
        story_text = story_text.join(str(paragraph) for paragraph in all_paragraphs)
        story_text = BeautifulSoup(story_text, "html.parser").get_text()
    story_text = story_text if not story_text == " " else None

    return (headline, story_text, article_url)


def write_articles(category, output_file_name, urls, no_of_articles, time_delay):
    path = output_file_name.split("/")
    output_file_name = os.path.join(path[0], f"{category}_{path[1]}")
    with open(output_file_name, "w") as csv_file:
        headers = ["headline", "text", "category", "url"]
        writer = csv.DictWriter(csv_file, delimiter="\t", fieldnames = headers)
        writer.writeheader()
        story_num = 0

        logging.info(f"Writing articles for {category} category...")
        for url in urls:
            headline, paragraphs, url = get_article_data(url)
            if paragraphs:
                writer.writerow({
                    headers[0]:headline, 
                    headers[1]:paragraphs, 
                    headers[2]:category, 
                    headers[3]:url,
                    })
                story_num+=1
                logging.info(f"Successfully wrote story number {story_num}")

            if story_num == no_of_articles:
                logging.info(
                    f"Requested total number of articles {no_of_articles} reached"
                    )
                logging.info(
                    f"Scraping done. A total of {no_of_articles} articles were scraped!"
                    )
                return
            if time_delay: 
                time.sleep(10)
        logging.info(
        f"Scraping done. A total of {story_num} articles were scraped!"
        )


def scrape(url, category, time_delay, articles_per_category, output_file_name):
    """
    Main function for scraping and writing articles to file

    input:
        :param output_file_name: file name where output is saved
        :param no_of_articles: number of user specified articles to scrape
        :param category_urls: all articles in a category
    """
    logging.info(f"Getting stories for {category}...")
    category_story_links = get_urls(url, category, time_delay, articles_per_category)
    
    # category_urls[category] = category_story_links
    # json.dump(category_story_links, open(f"{category}_story_links.json", "w"), indent=4)
    logging.info(f"{len(category_story_links)} stories found for {category} category")

    write_articles(
        category, 
        output_file_name, 
        category_story_links,
        articles_per_category,
        time_delay
    )

if __name__ == "__main__":

    logging.info("--------------------------------------")
    logging.info("Starting scraping...")
    logging.info("--------------------------------------")

    # initialize parser
    parser = get_parser()
    params, _ = parser.parse_known_args()

    # specify categories to scrape
    if params.categories != "all":
        categories = params.categories.upper().split(",")
        categories = {category: ALL_CATEGORIES[category] for category in categories}
    else:
        categories: dict = ALL_CATEGORIES or {}
        other_categories = get_topics(CONFIG["HOMEPAGE"], list(categories.values()))
        categories.update(other_categories)

    articles_per_category = params.no_of_articles
    if params.no_of_articles > 0 and params.spread:
        if "MOST_POPULAR" in categories:
            # most_popular only has one page and 10 articles only
            # subtract this from no_of_articles to be collected before spreading
            articles_per_category = (params.no_of_articles-10) // (len(categories)-1)
        else:
            articles_per_category = params.no_of_articles // len(categories)
        logging.info(f"Will collect at least {articles_per_category} stories per category")
    
    pool = multiprocessing.Pool()
    processes = [
        pool.apply_async(
            scrape,
            args=(
                url,
                category,
                params.time_delay,
                articles_per_category,
                params.output_file_name
            )
        ) for category, url in categories.items()
    ]
    result = [p.get() for p in processes]

    path = params.output_file_name.split("/")
    output_file_pattern = os.path.join(path[0], f"*_{path[1]}")
    category_file_names = glob.glob(output_file_pattern)

    reader = partial(pd.read_csv, sep="\t")
    all_dfs = map(reader, category_file_names)
    corpora = pd.concat(all_dfs).drop_duplicates(subset="url", keep="last")
    corpora.to_csv(params.output_file_name, sep="\t", index=False)

    if params.cleanup:
        for f in category_file_names:
            os.remove(f)
    