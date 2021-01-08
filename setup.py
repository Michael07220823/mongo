import setuptools

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mongo",
    version="0.0.3",
    author="Overcomer",
    author_email="michael31703@gmail.com",
    description="Mongodb operation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Michael07220823/mongo.git",
    keywords="mongo",
    python_requires='>=3',
    install_requires=['RandomWordGenerator', 'pypandoc', 'pymongo', 'new_timer'],
    license="MIT",
    packages=setuptools.find_packages(include=["mongo.py"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],

)