import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock

@pytest.fixture(autouse=True)
def setup_env():
    """Configura environment variables para todos os testes"""
    with patch.dict(os.environ, {
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_DB': 'test_db',
        'POSTGRES_USER': 'test_user',
        'POSTGRES_PASSWORD': 'test_pass',
        'POSTGRES_PORT': '5432',
        'MAIN_ENDPOINT': 'https://test.endpoint/'
    }):
        yield

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'cnpj': ['12345678000195', '98765432000187'],
        'razao_social': ['Empresa A', 'Empresa B'],
        'natureza_juridica': [1011, 2011],
        'cod_porte': ['03', '05']
    })