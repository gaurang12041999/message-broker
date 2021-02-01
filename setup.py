import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="WotNotMessageBroker",
    version="0.0.1",
    author="WotNot",
    author_email="wotnot@marutitech.com",
    description="This package will help to perform Rabbit MQ operations",
    long_description=long_description,
    long_description_content_type="text",
    url="https://github.com/gaurang19990412/message-broker",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python >= 3.5",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
    install_requires=["pika-pool==0.1.3", "pika==1.1.0"]
)
