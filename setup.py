
import setuptools

REQUIRES = ['gevent', 'requests', 'boto3']

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="s3_stress",
    version="1.0.0",
    author="Samuel Shapiro",
    author_email="samuel@vast.com",
    description="S3 load & stress tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/samuelsh/s3_stress",
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': ['s3_io_stress=s3_stress.s3_stress_runner:main',
                            's3_meta_stress=s3_stress.s3_put_meta:main',
                            's3_put_delete=s3_stress.s3_put_delete:main',
                            's3_put=s3_stress.s3_put:main',
                            's3_get=s3_stress.s3_get:main',
                            's3_delete=s3_stress.s3_delete:main',
                            'pcap2http=s3_stress.pcap2http:main',
                            's3_mp_upload=s3_stress.multipart_upload:main'
                            ],
    },
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    install_requires=REQUIRES
)
