from setuptools import find_packages, setup

setup(
    name="frappe_airflow",
    version="0.1.0",
    description="Frappe UI for Airflow configuration management",
    author="Fldrspro",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.11",
    install_requires=[
        "openpyxl>=3.1",
    ],
)
