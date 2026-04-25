from setuptools import setup, find_packages

setup(
    name="airflow_manager",
    version="0.1.0",
    description="Frappe UI for Airflow configuration management",
    author="Fldrspro",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.11",
)
