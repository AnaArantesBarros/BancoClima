import ee

def authenticate(project:str = ''):
    """Authenticate in earth engine.

    Keyword arguments:
    project -- earth engine project name (default '')
    """
    ee.Authenticate()
    ee.Initialize(project=project)
