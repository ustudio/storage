import os
import tempfile


def create_temp_nested_directory_with_files():
    # temp_directory/
    #   temp_input_one
    #   temp_input_two
    #   nested_temp_directory/
    #      nested_temp_input
    temp_dir = {}
    temp_dir["temp_directory"] = {"path": tempfile.mkdtemp()}
    temp_dir["temp_input_one"] = {
        "file": tempfile.NamedTemporaryFile(dir=temp_dir["temp_directory"]["path"])}
    temp_dir["temp_input_one"]["path"] = temp_dir["temp_input_one"]["file"].name
    temp_dir["temp_input_one"]["name"] = os.path.basename(temp_dir["temp_input_one"]["file"].name)

    temp_dir["temp_input_one"]["file"].write("FOO")
    temp_dir["temp_input_one"]["file"].flush()

    temp_dir["temp_input_two"] = {
        "file": tempfile.NamedTemporaryFile(dir=temp_dir["temp_directory"]["path"])}
    temp_dir["temp_input_two"]["path"] = temp_dir["temp_input_two"]["file"].name
    temp_dir["temp_input_two"]["name"] = os.path.basename(temp_dir["temp_input_two"]["file"].name)
    temp_dir["temp_input_two"]["file"].write("BAR")
    temp_dir["temp_input_two"]["file"].flush()

    temp_dir["nested_temp_directory"] = {
        "path": tempfile.mkdtemp(dir=temp_dir["temp_directory"]["path"])}
    temp_dir["nested_temp_directory"]["name"] = os.path.basename(
        temp_dir["nested_temp_directory"]["path"])

    temp_dir["nested_temp_input"] = {
        "file": tempfile.NamedTemporaryFile(dir=temp_dir["nested_temp_directory"]["path"])}
    temp_dir["nested_temp_input"]["path"] = temp_dir["nested_temp_input"]["file"].name
    temp_dir["nested_temp_input"]["name"] = os.path.basename(
        temp_dir["nested_temp_input"]["file"].name)

    temp_dir["nested_temp_input"]["file"].write("FOOBAR")
    temp_dir["nested_temp_input"]["file"].flush()

    return temp_dir
