#!/usr/bin/env python3

from subprocess import DEVNULL, STDOUT, check_call
from os.path import basename, realpath, isdir, isfile, dirname, relpath, join
from os import makedirs, getcwd, chdir
import tempfile
from urllib.request import urlretrieve
from textwrap import dedent
from base64 import b64encode
import argparse
import sys
import re
from core.shell import Shell
import logging
from PIL import Image
from core.github import GitHub

logger = logging.getLogger(__name__)


def read(file: str, mode='r'):
    with open(file, mode=mode) as f:
        return f.read()


def write(file: str, txt: str):
    with open(file, "w") as f:
        f.write(dedent(txt).strip())


class SchemasPy:
    EXT = ("png", "svg")

    def __init__(
            self,
            home=None
    ):
        self.home = home
        if self.home is None and isdir("schemaspy"):
            self.home = "schemaspy"
        if self.home is None:
            self.home = tempfile.mkdtemp()
        self.root = realpath(self.home) + "/"
        makedirs(self.root, exist_ok=True)

    def __dwn_if_needed(self, repo: str):
        url = GitHub.get_asset(repo, "jar")
        name = basename(url)
        file = self.root + name
        if isfile(file):
            return file
        logger.info("dwn "+file)
        urlretrieve(url, file)
        return file

    def report(self, file: str, out: str = None, imageformat: str = None):
        # https://github.com/schemaspy/schemaspy/issues/524#issuecomment-496010502
        if out is None:
            out = tempfile.mkdtemp()

        current_dir = getcwd()
        isProperties = file.endswith(".properties")
        self.__set_env(file)
        jar = self.__dwn_if_needed("schemaspy/schemaspy")

        file = realpath(file)
        cmd = ["java", "-jar", realpath(jar), "-o", out]
        out = realpath(out)

        if isProperties:
            new_dir = dirname(relpath(file))
            if current_dir != new_dir:
                chdir(new_dir)
                logger.info(f"$ cd {new_dir}")
            cmd.extend([
                "-configFile",
                basename(file),
            ])
        else:
            if self.root != new_dir:
                chdir(self.root)
                logger.info(f"$ cd {self.root}")
            name = basename(file).rsplit(".", 1)[0]
            cmd.extend([
                "-dp",
                self.root,
                "-db",
                file,
                "-cat",
                name,
                "-s",
                name,
                "-u",
                name
            ])
            if imageformat:
                cmd.extend(["-imageformat", imageformat])

        Shell.run(*cmd)
        if not isProperties:
            Shell.run("bash", self.root + "rename.sh", dirname(file) + "/", out)
        logger.info(out + "/index.html")
        chdir(current_dir)
        return out

    def __set_env(self, file: str):
        if file.endswith(".properties"):
            return

        driver = self.__dwn_if_needed("xerial/sqlite-jdbc")
        if driver.startswith(self.root):
            driver = driver[len(self.root):]

        write(self.root + "sqlite.properties", f'''
            driver=org.sqlite.JDBC
            description=SQLite
            driverPath={driver}
            connectionSpec=jdbc:sqlite:<db>
        ''')

        write(self.root + "schemaspy.properties", '''
            schemaspy.t=sqlite
            schemaspy.sso=true
        ''')

        write(self.root + "rename.sh", '''
            #!/bin/bash
            grep "$1" -l -r $2 | xargs -d '\\n' sed -i -e "s|${1}||g"
        ''')

    def save_diagram(self, db: str, img: str, size="compact"):
        ext = img.rsplit(".")[-1].lower()
        if ext not in SchemasPy.EXT:
            raise ValueError("Image format output must be: "+", ".join(SchemasPy.EXT))
        out = self.report(db, imageformat=ext)
        fl = f"{out}/diagrams/summary/relationships.real.{size}.{ext}"
        if not isfile(fl):
            logger.warning(f"{fl} not found")
            return None
        logger.info(f"$ cp {fl} {img}")
        if ext == "svg":
            svg = self.__parse_svg(fl)
            with open(img, "w") as f:
                f.write(svg)
            return
        if ext == "png":
            im = Image.open(fl)
            box = im.getbbox()
            box = list(box)
            box[3] = box[3] - 45
            gr = im.crop(tuple(box))
            gr.save(img)
            gr.close()
            im.close()

    def __parse_svg(self, fl):
        svg = read(fl)
        svg = re.sub(r"\n\s*<text[^>]+>Generated by SchemaSpy</text>", "", svg)
        svg = re.sub(r"\s*<a [^>]+>", "", svg)
        svg = re.sub(r"\s*</a>", "", svg)
        href_to_url = {}
        for href in re.findall(r'<image xlink:href="([^"]+)', svg):
            if href in href_to_url:
                continue
            image_binary = read(join(dirname(fl), href), mode='rb')
            image_base64 = b64encode(image_binary).decode("utf-8")
            href_to_url[href] = f"data:image/{href.rsplit('.')[-1]};base64,{image_base64}"
        for href, url in href_to_url.items():
            svg = svg.replace(f'"{href}"', f'"{url}"')

        def do_resize(m: re.Match):
            old = m.group(1)
            new = int(old) - 33
            r: str = re.sub(r"\s+", " ", m.group()).strip()
            r = r.replace(f'height="{old}pt"', f'height="{new}pt"')
            r = r.replace(f' {old}.00"', f' {new}.00"')
            return r

        svg = re.sub(
            r'<svg\s+width="\d+pt"\s+height="(\d+)pt"\s+viewBox="[\d\.\s]+\s+\1.00"',
            do_resize,
            svg
        )
        return svg
