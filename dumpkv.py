import itertools

import jinja2
from pathlib import Path
from typing import Dict, Tuple, Iterable

from eodatasets import serialise
import yaml

out_dir = Path('flat-datasets')
out_dir.mkdir(exist_ok=True)


def read_datasets(doc: Dict, name: str):
    sources = doc['lineage']['source_datasets']
    del doc['lineage']['source_datasets']

    yield name, list(serialise.as_flat_key_value(doc))

    for classifier, doc in sources.items():
        yield from read_datasets(doc, f"{name}.{classifier}")


def write_dataset(name, doc: Dict[str, object], known_keys: set):
    with (out_dir / f'{name}.txt').open('w') as f:
        f.writelines(
            (f'{key}: {doc[key]}\n' if key in doc else '\n')
            for key in sorted(known_keys)
        )


# language=HTML
template = jinja2.Template("""
<html >
 <meta charset="utf-8"> 
<style>
body {
    font-family: monospace;
    font-size: 0.7em;
}
table {
    width: 100%;
}
table tr:nth-child(even) {
    background-color: azure;
}

table tr:hover {
    background-color: aquamarine;
}
td {
    padding: 5px;
}
</style>

<table>
<thead>
    <tr>
    <th>key</th>
    {% for name, doc in datasets %}
        <th>{{name}}</th>
    {% endfor %}
    </tr>
</thead>
<tbody>
{% for key in known_keys|sort %}
    <tr>
        <td>{{key}}</td>
        {% for name, doc in datasets %}
            <td class='truncate'>{{doc[key] | string | truncate(30) }}</td>
        {% endfor %}    
    </tr>
{% endfor %}
</tbody>
</table>
</html>
""")


def write_table(path: Path, known_keys: set,
                datasets: Dict[str, Iterable[Tuple[str, object]]]):
    with path.open('w') as fd:
        fd.write(template.render(
            datasets=(
                sorted(
                    (name, dict(keyvals)) for name, keyvals in datasets.items()
                )
            ),
            known_keys=known_keys
        ))


all_datasets = None


def main():
    datasets: Dict[str, Iterable[Tuple[str, object]]] = {}

    with open('ARD-METADATA.yaml', 'r') as f:
        datasets.update(read_datasets(yaml.load(f), 'ard'))

    with open('nbar-scene.yaml', 'r') as f:
        datasets.update(read_datasets(yaml.load(f), 'nbar'))

    print(len(datasets))
    global all_datasets
    all_datasets = datasets
    all_keys = set(key for key, val in itertools.chain(*(datasets.values())))

    print(len(all_keys))

    for name, dataset in datasets.items():
        write_dataset(name, dict(dataset), all_keys)

    output_path = Path('table.html').absolute()
    write_table(output_path, all_keys, datasets)

    import webbrowser
    webbrowser.open(output_path.as_uri())


if __name__ == '__main__':
    main()
