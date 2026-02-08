# GOESMultiCloudPipeline

## Overview

The `GOESMultiCloudPipeline` class provides a complete end-to-end processing pipeline for GOES ABI L2+ data. It orchestrates the entire workflow from raw NetCDF files to CF-compliant Zarr stores, including metadata cataloging, regridding, and storage.

## Key Features

- **End-to-End Processing**: Complete pipeline from raw files to final Zarr store
- **Automated Workflow**: Minimal configuration required for standard processing
- **Parallel Processing**: Efficient multi-threaded file processing
- **Metadata Management**: Automatic metadata extraction and CF compliance
- **Quality Control**: Built-in data validation and quality checks
- **Progress Tracking**: Detailed logging and progress reporting
- **Configurable**: Flexible configuration through YAML files

## Architecture

### Pipeline Stages

1. **Discovery**: Scan and catalog GOES NetCDF files
2. **Validation**: Validate file integrity and metadata consistency
3. **Regridding**: Transform from geostationary to lat/lon grid
4. **Storage**: Store in CF-compliant Zarr format
5. **Quality Control**: Validate output and generate reports

### Data Flow

```
Raw NetCDF Files → Metadata Catalog → Regridding → Zarr Store → QC Reports
```

## Class Structure

### Initialization

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESMultiCloudPipeline

# Initialize with configuration files
pipeline = GOESMultiCloudPipeline(
    data_config='./configurations/data/goesmulticloudnc.yaml',
    store_config='./configurations/store/goesmulticloudzarr.yaml',
    output_dir='./output/'
)

# Or with explicit parameters
pipeline = GOESMultiCloudPipeline(
    data_dir='/data/goes/',
    output_dir='./output/',
    platforms=['GOES-East', 'GOES-West'],
    bands=[1, 2, 3, 7, 14],
    target_resolution=0.02
)
```

### Constructor Parameters

```python
GOESMultiCloudPipeline(
    data_config: Optional[str] = None,
    store_config: Optional[str] = None,
    data_dir: Optional[str] = None,
    output_dir: str = './output/',
    platforms: Optional[list] = None,
    bands: Optional[list] = None,
    target_resolution: float = 0.02,
    parallel_workers: int = 4,
    cache_weights: bool = True
)
```

**Parameters:**
- `data_config`: Path to data configuration YAML file
- `store_config`: Path to store configuration YAML file
- `data_dir`: Override data directory from config
- `output_dir`: Output directory for Zarr stores
- `platforms`: GOES platforms to process (default: all)
- `bands`: ABI bands to process (default: all 16)
- `target_resolution`: Output grid resolution in degrees
- `parallel_workers`: Number of parallel processing workers
- `cache_weights`: Whether to cache regridding weights

## Core Methods

### Pipeline Execution

#### Full Pipeline Run

```python
# Run complete pipeline
results = pipeline.run()

# Results include:
# - processed_files: List of successfully processed files
# - failed_files: List of failed files with error messages
# - statistics: Processing statistics and timing
# - output_path: Path to generated Zarr store
```

#### Stage-by-Stage Processing

```python
# Individual pipeline stages
pipeline.discover_files()           # Scan and catalog files
pipeline.validate_files()           # Validate file integrity
pipeline.process_files()            # Process all files
pipeline.generate_reports()         # Generate QC reports
```

#### Custom Processing

```python
# Process specific date range
results = pipeline.process_date_range(
    start_date='2024-01-01',
    end_date='2024-01-31',
    platforms=['GOES-East']
)

# Process specific files
results = pipeline.process_files([
    '/data/GOES18/2024/01/01/file1.nc',
    '/data/GOES18/2024/01/01/file2.nc'
])
```

### Configuration Management

#### Load Configuration

```python
# Load from configuration files
pipeline = GOESMultiCloudPipeline.from_configs(
    data_config='./configurations/data/goesmulticloudnc.yaml',
    store_config='./configurations/store/goesmulticloudzarr.yaml'
)

# Override specific parameters
pipeline.set_platforms(['GOES-East', 'GOES-West'])
pipeline.set_bands([1, 2, 3, 7, 14])
pipeline.set_target_resolution(0.01)  # Higher resolution
```

#### Dynamic Configuration

```python
# Update configuration at runtime
pipeline.update_config({
    'parallel_workers': 8,
    'cache_weights': True,
    'output_dir': './new_output/'
})

# Get current configuration
current_config = pipeline.get_config()
print(f"Current platforms: {current_config['platforms']}")
```

### Monitoring and Progress

#### Progress Tracking

```python
# Enable progress reporting
pipeline.enable_progress_reporting()

# Run with progress callbacks
def progress_callback(progress):
    print(f"Progress: {progress['percent_complete']:.1f}%")
    print(f"Files processed: {progress['files_processed']}")
    print(f"Current file: {progress['current_file']}")

results = pipeline.run(progress_callback=progress_callback)
```

#### Logging and Diagnostics

```python
# Set logging level
pipeline.set_log_level('INFO')

# Enable detailed logging
pipeline.enable_debug_logging()

# Get processing statistics
stats = pipeline.get_statistics()
print(f"Total files: {stats['total_files']}")
print(f"Processing time: {stats['processing_time']:.2f} seconds")
print(f"Average file time: {stats['avg_file_time']:.2f} seconds")
```

## Configuration Schema

### Pipeline Configuration

```yaml
# Pipeline-specific configuration
pipeline:
  # Processing parameters
  parallel_workers: 4
  batch_size: 100
  memory_limit: "8GB"
  
  # Progress tracking
  enable_progress: true
  progress_interval: 10  # seconds
  
  # Quality control
  enable_qc: true
  qc_thresholds:
    min_coverage_fraction: 0.8
    max_missing_fraction: 0.1
    temporal_tolerance: 300  # seconds
  
  # Output options
  generate_reports: true
  save_intermediate: false
  compression_level: 5
  
  # Error handling
  max_retries: 3
  retry_delay: 5  # seconds
  continue_on_error: true
```

### Integration with Existing Configs

The pipeline integrates with existing configuration files:

- **Data Configuration**: Uses `goesmulticloudnc.yaml` for file access patterns
- **Store Configuration**: Uses `goesmulticloudzarr.yaml` for Zarr store setup
- **Pipeline Configuration**: Optional pipeline-specific parameters

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESMultiCloudPipeline

# Simple pipeline execution
pipeline = GOESMultiCloudPipeline(
    data_dir='/data/goes/',
    output_dir='./output/'
)

results = pipeline.run()
print(f"Processed {len(results['processed_files'])} files")
print(f"Output: {results['output_path']}")
```

### Advanced Configuration

```python
# Advanced configuration
pipeline = GOESMultiCloudPipeline(
    data_config='./configurations/data/goesmulticloudnc.yaml',
    store_config='./configurations/store/goesmulticloudzarr.yaml',
    output_dir='./output/',
    platforms=['GOES-East', 'GOES-West'],
    bands=[1, 2, 3, 7, 14],
    target_resolution=0.01,
    parallel_workers=8,
    cache_weights=True
)

# Custom processing with progress tracking
def progress_handler(progress):
    if progress['percent_complete'] % 10 == 0:
        print(f"Progress: {progress['percent_complete']}%")

results = pipeline.run(progress_callback=progress_handler)

# Generate detailed reports
pipeline.generate_qc_report('./qc_report.html')
pipeline.generate_processing_log('./processing.log')
```

### Batch Processing

```python
# Process multiple date ranges
date_ranges = [
    ('2024-01-01', '2024-01-31'),
    ('2024-02-01', '2024-02-29'),
    ('2024-03-01', '2024-03-31')
]

for start_date, end_date in date_ranges:
    print(f"Processing {start_date} to {end_date}")
    
    results = pipeline.process_date_range(start_date, end_date)
    
    if results['failed_files']:
        print(f"Failed files: {len(results['failed_files'])}")
        for failed in results['failed_files']:
            print(f"  {failed['file']}: {failed['error']}")
```

### Custom Processing Logic

```python
# Extend pipeline with custom processing
class CustomGOESPipeline(GOESMultiCloudPipeline):
    def custom_processing_step(self, observation):
        """Add custom processing logic"""
        # Apply custom filters or transformations
        return observation
    
    def process_observation(self, observation):
        """Override standard processing"""
        # Custom preprocessing
        observation = self.custom_processing_step(observation)
        
        # Standard processing
        return super().process_observation(observation)

# Use custom pipeline
custom_pipeline = CustomGOESPipeline(
    data_dir='/data/goes/',
    output_dir='./output/'
)

results = custom_pipeline.run()
```

## Error Handling and Recovery

### Robust Error Handling

```python
# Configure error handling
pipeline = GOESMultiCloudPipeline(
    data_dir='/data/goes/',
    output_dir='./output/',
    max_retries=3,
    retry_delay=5,
    continue_on_error=True
)

# Run with error recovery
results = pipeline.run()

# Check failed files
if results['failed_files']:
    print(f"Failed to process {len(results['failed_files'])} files:")
    for failed in results['failed_files']:
        print(f"  {failed['file']}: {failed['error']}")
        
    # Retry failed files
    retry_results = pipeline.retry_failed_files(results['failed_files'])
```

### Validation and QC

```python
# Enable comprehensive quality control
pipeline.enable_qc(
    min_coverage_fraction=0.8,
    max_missing_fraction=0.1,
    temporal_tolerance=300
)

# Run with QC
results = pipeline.run()

# Get QC report
qc_report = pipeline.get_qc_report()
print(f"QC passed: {qc_report['passed_files']}")
print(f"QC failed: {qc_report['failed_files']}")
print(f"Coverage statistics: {qc_report['coverage_stats']}")
```

## Performance Optimization

### Memory Management

```python
# Configure for memory-constrained systems
pipeline = GOESMultiCloudPipeline(
    data_dir='/data/goes/',
    output_dir='./output/',
    parallel_workers=2,        # Fewer workers
    batch_size=50,           # Smaller batches
    memory_limit="4GB"
)

# Enable memory-efficient processing
pipeline.enable_memory_optimization()
```

### Parallel Processing

```python
# Configure for high-performance systems
pipeline = GOESMultiCloudPipeline(
    data_dir='/data/goes/',
    output_dir='./output/',
    parallel_workers=16,       # More workers
    batch_size=200,          # Larger batches
    cache_weights=True        # Cache regridding weights
)

# Optimize for I/O
pipeline.enable_io_optimization()
```

## Integration Examples

### Complete Workflow

```python
from goesdatabuilder.pipelines.goesmulticloudpipeline import GOESMultiCloudPipeline
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Initialize pipeline
pipeline = GOESMultiCloudPipeline(
    data_config='./configurations/data/goesmulticloudnc.yaml',
    store_config='./configurations/store/goesmulticloudzarr.yaml',
    output_dir='./goes_output/',
    platforms=['GOES-East', 'GOES-West'],
    bands=[1, 2, 3, 7, 14],
    target_resolution=0.02,
    parallel_workers=4
)

# Add progress tracking
def progress_callback(progress):
    print(f"Progress: {progress['percent_complete']:.1f}%")
    if progress['current_file']:
        print(f"Processing: {progress['current_file']}")

# Run pipeline
try:
    results = pipeline.run(progress_callback=progress_callback)
    
    # Report results
    print(f"\nPipeline completed successfully!")
    print(f"Processed files: {len(results['processed_files'])}")
    print(f"Failed files: {len(results['failed_files'])}")
    print(f"Output store: {results['output_path']}")
    print(f"Processing time: {results['statistics']['processing_time']:.2f} seconds")
    
    # Generate reports
    pipeline.generate_qc_report('./qc_report.html')
    pipeline.generate_processing_log('./processing.log')
    
except Exception as e:
    print(f"Pipeline failed: {e}")
    pipeline.generate_error_report('./error_report.txt')
```

### Scheduled Processing

```python
# Example for scheduled daily processing
import schedule
import time
from datetime import datetime, timedelta

def daily_processing():
    """Process yesterday's GOES data"""
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    
    pipeline = GOESMultiCloudPipeline(
        data_dir='/data/goes/',
        output_dir=f'./output/{date_str}/',
        platforms=['GOES-East', 'GOES-West'],
        bands=[1, 2, 3, 7, 14]
    )
    
    # Process yesterday's data
    results = pipeline.process_date_range(date_str, date_str)
    
    print(f"Processed {date_str}: {len(results['processed_files'])} files")

# Schedule daily processing
schedule.every().day.at("02:00").do(daily_processing)

print("Scheduled daily GOES processing at 02:00")
while True:
    schedule.run_pending()
    time.sleep(60)
```

## API Reference

### Constructor

```python
GOESMultiCloudPipeline(
    data_config: Optional[str] = None,
    store_config: Optional[str] = None,
    data_dir: Optional[str] = None,
    output_dir: str = './output/',
    platforms: Optional[list] = None,
    bands: Optional[list] = None,
    target_resolution: float = 0.02,
    parallel_workers: int = 4,
    cache_weights: bool = True
)
```

### Class Methods

```python
from_configs(data_config: str, store_config: str) -> 'GOESMultiCloudPipeline'
run(progress_callback: Optional[callable] = None) -> dict
process_date_range(start_date: str, end_date: str, platforms: Optional[list] = None) -> dict
process_files(file_list: list) -> dict
retry_failed_files(failed_files: list) -> dict
```

### Configuration Methods

```python
set_platforms(platforms: list) -> None
set_bands(bands: list) -> None
set_target_resolution(resolution: float) -> None
update_config(config: dict) -> None
get_config() -> dict
```

### Monitoring Methods

```python
enable_progress_reporting() -> None
set_log_level(level: str) -> None
enable_debug_logging() -> None
get_statistics() -> dict
generate_qc_report(output_path: str) -> None
generate_processing_log(output_path: str) -> None
generate_error_report(output_path: str) -> None
```

### Quality Control Methods

```python
enable_qc(min_coverage_fraction: float = 0.8, 
          max_missing_fraction: float = 0.1,
          temporal_tolerance: int = 300) -> None
get_qc_report() -> dict
validate_output(store_path: str) -> dict
```

### Performance Methods

```python
enable_memory_optimization() -> None
enable_io_optimization() -> None
set_batch_size(size: int) -> None
set_parallel_workers(workers: int) -> None
```

## Best Practices

### Configuration Management

1. **Use Configuration Files**: Store parameters in YAML files for reproducibility
2. **Environment Variables**: Use environment variables for paths and credentials
3. **Version Control**: Keep configuration files in version control
4. **Documentation**: Document custom configurations and their purpose

### Performance Optimization

1. **Batch Processing**: Process files in batches for better memory usage
2. **Weight Caching**: Enable weight caching for repeated processing
3. **Parallel Workers**: Adjust worker count based on available CPU cores
4. **Memory Limits**: Set appropriate memory limits for your system

### Error Handling

1. **Continue on Error**: Set `continue_on_error=True` for batch processing
2. **Retry Logic**: Configure appropriate retry counts and delays
3. **Logging**: Enable detailed logging for debugging
4. **QC Reports**: Generate quality control reports for validation

### Monitoring

1. **Progress Tracking**: Use progress callbacks for long-running processes
2. **Statistics**: Monitor processing statistics and performance metrics
3. **Resource Usage**: Monitor memory and CPU usage during processing
4. **Output Validation**: Validate output stores after processing

This pipeline provides a robust, configurable solution for processing GOES ABI data from raw NetCDF files to analysis-ready Zarr stores.
