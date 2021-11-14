import json
from typing import List, Tuple, Optional, Union

import pandas as pd
import numpy as np
import plotly
from elastipy import Search, query, connections

pd.options.plotting.backend = "plotly"
plotly.templates.default = "plotly_dark"

connections.set("default", {"timeout": 60})