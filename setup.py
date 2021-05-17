import setuptools

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="new_mongo",
    version="0.0.6",
    author="Overcomer",
    author_email="michael31703@gmail.com",
    description="Mongodb operation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Michael07220823/mongo.git",
    keywords="mongo",
    install_requires=['pypandoc', 'Random-Word-Generator', 'dnspython', 'pymongo', 'new_timer'],
    license="MIT License",
    packages=setuptools.find_packages(include=["new_mongo"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],

)