from setuptools import setup, find_packages

setup(
    name="talk_to_salesforce",
    version="2.0.1",
    packages=find_packages(),
    install_requires=[
        "click",
        "google-cloud-bigquery",
        "google-cloud-secret-manager",
        "google-cloud-storage",
        "Jinja2",
        "requests",
    ],
    entry_points={
        "console_scripts": [
            "talk-to-salesforce = talk_to_salesforce:cli",    
        ],
    },
)
