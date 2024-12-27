"""Module providing constants and stage dependencies
"""
BASE='/home/tunghx/workspace' # TODO
DEPENDENCY_GRAPH = {
  'HIMAWARI_pipeline': {
    "HimaPreprocess": {
      "dependJobs": [],
      "nextJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "DEMPreprocess"}]
    },
    "DEMPreprocess": {
      "dependJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "HimaPreprocess"}],
      "nextJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "CaliHimawariOnly"}]
    },
    "CaliHimawariOnly": {
      "dependJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "DEMPreprocess"}],
      "nextJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "PostProcess"}]
    },
    "PostProcess": {
      "dependJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "CaliHimawariOnly"}],
      "nextJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "ODCImport"}]
    },
    "ODCImport": {
      "dependJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "PostProcess"}],
      "nextJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "TerracottaImport"}]
    },
    "TerracottaImport": {
      "dependJobs": [{"pipeline": "HIMAWARI_pipeline", "stage": "ODCImport"}],
      "nextJobs": []
    }
  }
}
