# Automatically created by: scrapyd-deploy

from setuptools import setup, find_packages

setup(
    name='NewsCrawler',
    version='1.0',
    packages=find_packages(),
    entry_points={'scrapy': ['settings = crawlers.settings']}, install_requires=['beautifulsoup4', 'scrapy',
                                                                                 'requests', 'selenium',
                                                                                 'mysql', 'twisted']
)
