#
#
#
#
"""
setup.py file for Cheetah GUI
"""

from setuptools import find_packages, setup


version_fh = open("src/cheetah/__init__.py", "r")
version = version_fh.readlines()[-1].split("=")[1].strip().split('"')[1]
version_fh.close()
setup(
    name="cheetah",
    version=version,
    url="https://github.com/cheetahdevteam/cheetah-gui",
    license="GNU General Public License v3.0",
    author="Chetah Dev Team",
    author_email="alexandra.tolstikova@desy.de",
    description="Blah blah blah",
    long_description=(
        """
        Cheetah blah blah blah
        """
    ),
    install_requires=[
        "click",
        "h5py",
        "mypy-extensions",
        "numpy",
        "pyyaml",
        "scipy",
        "typing_extensions",
    ],
    extras_require={
        "qt": ["pyqt5", "pyqtgraph"],
        "docs": ["mkdocs", "mkdocstring", "mkdocs-click", "mkdocs-material"],
    },
    entry_points={
        "console_scripts": [],
        "gui_scripts": [
            "cheetah_gui.py=cheetah.gui:main",
        ],
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    platforms="any",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Natural Language :: English",
        "Intended Audience :: Science/Research",
    ],
)
