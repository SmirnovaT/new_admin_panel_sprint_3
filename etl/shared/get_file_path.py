import os


def get_path_file(folder_name, file_name):
    """Функция получения абcолютного пути к файлу"""
    current_dir = os.path.abspath(__file__)
    parent_dir = os.path.dirname(os.path.dirname(current_dir))
    template_path = os.path.join(parent_dir, folder_name, file_name)
    return template_path
