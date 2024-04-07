from typing import Union

from dev.data_loader import LocalData
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fetch.article import FetchArticle


from feed.profile_feed import parse_feed_metadata
from fetch.async_fetcher import fetch_all_feeds
from fetch.fetch_feeds import load_feed_list
from fetch.fetch_rss import get_articles, get_feed
from fetch.section import get_section_metadata
from fetch.channels import load_channel_list

app = FastAPI()

# origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------------------



''' TEST
http://127.0.0.1:8000/categories
'''

@app.get("/documents")
def documents():
    """
    Return a list of document ids
    """
    return LocalData().get_documents()


@app.get("/feed_list")
def feed_list():
    return LocalData().get_feed_list()


''' TEST
http://127.0.0.1:8000/channel_list?channel_id=other

'''
@app.get("/channel_list")
def channel_list(channel_id: Union[str, None] = None):
    """
    Return article for given url
    """
    print(f"channel_list() channel_id: {channel_id}")
    if channel_id:
        # return {"Foo": "Bar"}
        return load_channel_list(channel_id)
    return {"Error": "no channel id"}




@app.get("/article")
def article():
    """
    Return one (hard-coded) example article
    TODO: Deprecate and delete this route & method
    """
    return LocalData().get_article()


''' TEST:
http://127.0.0.1:8000/get_article?url=https://news.yahoo.com/republican-conspiracies-biden-fbi-poof-183534324.html
'''
@app.get("/get_article")
def get_article(url: Union[str, None] = None):
    """
    Return article for given url
    """
    if url:
        return FetchArticle().fetch(url)


"""
TODO : Needed routes / endpoints

get_section_channels 
- given input (section/category name) -> return channels (name & url) in that section

fetch_feed
- given input (feed url) -> fetch feed and return


"""


@app.get("/fetch_feed")
def fetch_feed(q: Union[str, None] = None):
    print(f"called fetch_feed for url: {q}")
    if q:
        print(f"query: {q}")
        # return get_userdefined_feed(q)
        return get_feed(q)


# TODO - combine with below; Refactor to combine with categories
@app.get("section_channels")
def section_channels(q: Union[str, None] = None):
    if q:
        return get_section_metadata(q)


# ================================================================================

# TODO - combine with above
@app.get("/v1/section_channels")
def section_channels():
    return LocalData().load_section_channels()


