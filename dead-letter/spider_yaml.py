import yaml


class YamlAdapter:
    def __init__(self, file_path: str) -> None:
        self.file_path: str = file_path

    def get_content(self) -> dict:
        with open(self.file_path, "r") as stream:
            return yaml.safe_load(stream)