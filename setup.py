import setuptools


setuptools.setup(
    name="hero_db_utils",
    version="1.3.0",
    description="Database utilities for projects related to the AIO-hero platform.",
    install_requires=[
        "boto3>=1.18.25",
        "requests>=2.25.0"
    ],
    packages=setuptools.find_packages(exclude=["tests.*", "tests"]),
)