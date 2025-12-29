import json
from collections import OrderedDict


def is_snake(s):
    return isinstance(s, str) and all(c.isalnum() or c == "_" for c in s)


class Owner:
    def  __init__(self, name, email=None):
        self.name = name
        self.email = email


class Exposure:
    def __init__(self, *args, maturity="low", **kwargs):
        name = kwargs.get("name")
        if not is_snake(name):
            for v in args:
                if is_snake(v):
                    name = v
                    break
        self.name = name.lower().replace(" ", "_") if name else name
        self.label = kwargs.get("label")
        self.type = kwargs.get("type")
        self.maturity = maturity
        self.url = kwargs.get("url")
        if not self.url:
            for v in args:
                if v.startswith("http"):
                    self.url = v

        self.description = kwargs.get("description")
        self.depends_on = kwargs.get("depends_on", [])
        self.owner = kwargs.get("owner")

    @property
    def to_dict(self):
        d = OrderedDict()
        d["name"] = self.name
        d["label"] = self.label
        d["type"] = self.type
        d["maturity"] = self.maturity
        d["url"] = self.url
        d["description"] = self.description
        d["depends_on"] = self.depends_on
        d["owner"] = {
            "name": self.owner,  # .name,
            "email": None  # self.owner.email
        }

        return dict(d)

    def __repr__(self):
        return f"Exposure({self.name}: {self.type}, owner={self.owner})"

class ImpactReport:
    def __init__(self, graph):
        self.graph = graph

    def save(self, output_path):
        with open(output_path, "w") as f:
            json.dump(self.graph, f, indent=2)

    def write_report(self, output_path):
        with open(output_path + ".tsv", "w") as f:
            f.write(
                "\t".join(["dataset", "owner", "created_by", "updated_by", "email"])
            )
            for dataset, d in self.graph.items():
                metadata = d["meta"]
                downstream = d["downstream"]
                f.write(
                    "\t".join(map(str,
                                  [dataset, metadata["owner"], metadata["created_by"], metadata["updated_by"],
                                   metadata["email"]]
                                  )) + "\n"
                )
