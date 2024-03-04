import datasets
import sys

MAX_SIZE = 40000000000

# =============================================================================

# To load a dataset from local path using an argument from CLI
if len(sys.argv) > 1:
    path_to_datasets = sys.argv[1]
else:
    path_to_datasets = None

# =============================================================================

# List of all available datasets
list_of_datasets = datasets.list_datasets(with_community_datasets=False)  # depcrecated

# =============================================================================

# Retain only dataset names
dataset_names = []
for i, dataset in enumerate(list_of_datasets):
    dataset = dataset.split("/")[-1]
    dataset_names.append(dataset)

# print(dataset_names)

# Example of a chosen dataset from above
option = dataset_names[0]
print(option)

# =============================================================================


# Get functions from datasets-viewer
def get_confs(opt):
    "Get the list of confs for a dataset."
    if path_to_datasets is not None and opt is not None:
        path = path_to_datasets + opt
    else:
        path = opt

    dataset_module = datasets.load.dataset_module_factory(path)
    # Get dataset builder class from the processing script
    builder_cls = datasets.load.import_main_class(dataset_module.module_path)
    # Instantiate the dataset builder
    confs = builder_cls.BUILDER_CONFIGS
    if confs and len(confs) > 1:
        return confs
    else:
        return []


def get(opt, conf=None):
    "Get a dataset from name and conf"
    if path_to_datasets is not None:
        path = path_to_datasets + opt
    else:
        path = opt

    dataset_module = datasets.load.dataset_module_factory(path)
    # Get dataset builder class from the processing script
    builder_cls = datasets.load.import_main_class(dataset_module.module_path)
    if conf:
        builder_instance = builder_cls(
            name=conf, cache_dir=path if path_to_datasets is not None else None
        )
    else:
        builder_instance = builder_cls(
            cache_dir=path if path_to_datasets is not None else None
        )
    fail = False
    if path_to_datasets is not None:
        dts = datasets.load_dataset(
            path,
            name=(
                builder_cls.BUILDER_CONFIGS[0].name
                if builder_cls.BUILDER_CONFIGS
                else None
            ),
        )
        dataset = dts

    elif (
        builder_instance.manual_download_instructions is None
        and builder_instance.info.size_in_bytes is not None
        and builder_instance.info.size_in_bytes < MAX_SIZE
    ):
        builder_instance.download_and_prepare()
        dts = builder_instance.as_dataset()
        dataset = dts
    else:
        dataset = builder_instance
        fail = True
    return dataset, fail


configs = get_confs(option)
print(configs)

dataset = get(option, configs)
print(dataset)
