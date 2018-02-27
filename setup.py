from setuptools import setup

setup(
    name="uwa publications scraper",
    version="0.0.1",
    author="Robert Honeybul",
    author_email="honeybulr@gmail.com",
    description=("""
    Web Scraping Package that is Capable of scraping all recent UWA publications.
    """),
    license="BSD",
    keywords="uwa publications scraper",
    packages=['get_publications'],
    dependencies=['bs4']
)