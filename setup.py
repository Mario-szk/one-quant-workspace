from setuptools import setup

with open("README.md", "r",encoding="utf8") as fh:
    long_description = fh.read()

setup(
    name='one_quant_workspace',
    version='0.0.1',
    author='onewayforever',
    author_email='onewayforever@163.com',
    url='https://github.com/onewayforever/one-quant-task',
    description=u'quant engine for analyze stock ',
    scripts=["scripts/oqwspace"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    #packages=find_packages(),
    packages=['one_quant_workspace'],
    install_requires=[
        'one_quant_data>=0.1.1',
        'tushare>=1.2.26',
        'progressbar',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'numpy'
    ]
)

