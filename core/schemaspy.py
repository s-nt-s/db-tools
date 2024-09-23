from os.path import basename, realpath, isdir, isfile, dirname, getmtime, join
from os import makedirs, getcwd, chdir, remove
import tempfile
from urllib.request import urlretrieve
from textwrap import dedent
from base64 import b64encode
import re
from core.shell import Shell
import logging
from PIL import Image
from core.github import GitHub
from core.filemanager import FileManager
from configparser import ConfigParser
from os.path import expandvars
from typing import Union
from datetime import datetime
from glob import glob

logger = logging.getLogger(__name__)
FM = FileManager.get()


def days_from_updated(archivo):
    if not isfile(archivo):
        return 999999999999
    mtime = datetime.fromtimestamp(getmtime(archivo))
    now = datetime.now()
    return (now - mtime).days


def mychdir(d: str):
    if len(d) == 0:
        return
    if d != getcwd():
        chdir(d)
        logger.info(f"$ cd {d}")


def read(file: str, mode='r'):
    with open(file, mode=mode) as f:
        return f.read()


def write(file: str, txt: str):
    with open(file, "w") as f:
        f.write(dedent(txt).strip())


def find_config(config: ConfigParser, field):
    for s in config.sections():
        v = config[s].get(field)
        if v is not None:
            return s, field, v.strip()
    raise ValueError(f"{field} not found")


def find_arg_env(config: Union[ConfigParser, str]):
    if isinstance(config, str):
        config = FM.load(config)
    for s in config.sections():
        for k, v in config.items(s):
            if not k.startswith("schemaspy."):
                continue
            new_v = expandvars(v)
            if new_v != v:
                yield k.split('.', 1)[-1], v, new_v

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

    def __dwn_if_needed(self, repo: str, sufix):
        url = GitHub.get_asset(repo, sufix)
        name = basename(url)
        file = self.root + name
        if isfile(file):
            return file
        logger.info("dwn "+file)
        urlretrieve(url, file)
        return file

    def report(self, file: str, out: str = None, imageformat: str = None, include: Union[str, None] = None, rows: bool = False):
        if out is None:
            out = tempfile.mkdtemp()

        current_dir = getcwd()
        isProperties = file.endswith(".properties")
        self.__set_env(file)
        jar = self.__get_schemaspy_jar()

        file = realpath(file)
        cmd = ["java", "-jar", realpath(jar), "-o", out, "-dp", self.root]
        out = realpath(out)
        expand = False

        if isProperties:
            cmd.extend([
                "-configFile",
                realpath(file),
            ])
            for k, v, new_v in find_arg_env(file):
                expand = True
                cmd.extend(['-'+k, v])
        else:
            # https://github.com/schemaspy/schemaspy/issues/524#issuecomment-496010502
            cmd.extend([
                "-configFile",
                "schemaspy-sqlite.properties",
                "-db",
                file,
            ])
        if imageformat:
            cmd.extend(["-imageformat", imageformat])
        if include:
            cmd.extend(["-i", include])
        if not rows:
            cmd.append("--norows")

        mychdir(self.root)
        Shell.run(*cmd, expand=True)
        if not isProperties:
            Shell.run("bash", self.root + "rename.sh", dirname(file) + "/", out)
        html = out + "/index.html"
        if isfile(html):
            logger.info(html)
        chdir(current_dir)
        return out

    def __get_schemaspy_jar(self):
        jars = list(glob(self.root + "schemaspy-*.jar"))
        if len(jars) == 1 and days_from_updated(jars[0]) < 30:
            return jars[0]
        for j in jars:
            remove(j)
        return self.__dwn_if_needed("schemaspy/schemaspy", ".jar")

    def __set_env(self, file: str):
        if file.endswith(".properties"):
            config: ConfigParser = FM.load(realpath(file))
            _, _, value = find_config(config, "schemaspy.t")
            self.__create_properties(value)
            return
        self.__create_properties("sqlite")

        write(self.root + "schemaspy.properties", '''
            schemaspy.t=sqlite
            schemaspy.sso=true
        ''')

    def __create_properties(self, name: str):
        config: ConfigParser = FM.load(f"schemaspy/template/{name}.properties")
        if config is None:
            return
        path = str(FM.resolve_path(f"schemaspy/{name}.properties"))
        if days_from_updated(path) < 30:
            return
        section, field, value = find_config(config, "driverPath")

        driver, sufix = value.split()
        driver = self.__dwn_if_needed(driver, sufix)
        if driver.startswith(self.root):
            driver = driver[len(self.root):]
        config[section][field] = driver
        FM.dump(path, config)


    def save_diagram(self, db: str, img: str, size="compact", include: Union[str, None] = None, rows: bool = False):
        ext = img.rsplit(".")[-1].lower()
        if ext not in SchemasPy.EXT:
            raise ValueError("Image format output must be: "+", ".join(SchemasPy.EXT))
        out = self.report(db, imageformat=ext, include=include, rows=rows)
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
