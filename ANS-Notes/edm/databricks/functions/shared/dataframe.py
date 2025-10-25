from pyspark.sql import DataFrame
import logging


def _is_data_column(name: str) -> bool:
    return not _is_special_column(name)


def _is_key_column(name: str) -> bool:
    return True if name == '_ID' or name[-3:] == '_ID' else False


def _is_internal_column(name: str) -> bool:
    return True if name[:1] == '_' and not _is_key_column(name) else False


def _is_special_column(name: str) -> bool:
    return _is_internal_column(name) or _is_key_column(name)


def get_key_columns(self, func=_is_key_column) -> list:
    return [c for c in self.columns if func(c)]


def get_internal_columns(self, func=_is_internal_column) -> list:
    return [c for c in self.columns if func(c)]


def get_special_columns(self, func=_is_special_column) -> list:
    return [c for c in self.columns if func(c)]


def get_data_columns(self, func=_is_data_column) -> list:
    return [c for c in self.columns if func(c)]


def present(self):
    self.display()
    return self


def execute(self, f, *args, **kwargs):
    logger = logging.getLogger('edm')
    try:
        result = f(self, *args, **kwargs)
        logger.debug(f'{f.__name__}: {result}')
    except Exception as e:
        logger.error(f.__name__)
        raise (e)
    return self


def transform(self, f, *args, **kwargs):
    logger = logging.getLogger('edm')
    try:
        logger.debug(f'{f.__name__}')
        result = f(self, *args, **kwargs)
    except Exception as e:
        logger.error(f.__name__)
        raise (e)
    return result


def transform_if(self, cond, f, *args, **kwargs):
    return transform(self, f, *args, **kwargs) if cond else self


DataFrame.get_key_columns = get_key_columns
DataFrame.get_internal_columns = get_internal_columns
DataFrame.get_special_columns = get_special_columns
DataFrame.get_data_columns = get_data_columns
DataFrame.present = present
DataFrame.execute = execute
DataFrame.transform = transform
DataFrame.transform_if = transform_if
