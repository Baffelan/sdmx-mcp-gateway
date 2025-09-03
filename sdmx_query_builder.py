"""
SDMX Query Builder with correct dimension handling.

This module ensures proper SDMX data query construction following the 
SDMX 2.1 REST API specification:
- Correct dimension ordering based on DSD
- Proper handling of time dimensions via startPeriod/endPeriod
- Empty strings for non-filtered dimensions (maintaining dot count)
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging
from config import SDMX_BASE_URL

logger = logging.getLogger(__name__)


@dataclass
class SDMXQuerySpec:
    """Specification for building an SDMX data query."""
    dataflow_id: str
    agency_id: str
    version: str
    dimension_values: Dict[str, str]  # Dimension ID -> value
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    provider: str = "all"
    format_type: str = "structurespecificdata"
    

class SDMXQueryBuilder:
    """Builder for correct SDMX data queries."""
    
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or SDMX_BASE_URL).rstrip('/')
    
    def build_data_key(self,
                      dimension_order: List[str],
                      dimension_values: Dict[str, str],
                      exclude_time: bool = True) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Build a properly formatted SDMX data key.
        
        Args:
            dimension_order: Ordered list of dimension IDs from the DSD
            dimension_values: Dictionary of dimension ID -> value
            exclude_time: If True, time dimension is handled via query params
            
        Returns:
            Tuple of (key, start_period, end_period)
        """
        key_parts = []
        start_period = None
        end_period = None
        
        for dim_id in dimension_order:
            # Check if this is a time dimension
            if exclude_time and dim_id in ['TIME_PERIOD', 'TIME', 'REF_PERIOD']:
                # Handle time dimension separately
                if dim_id in dimension_values:
                    time_value = dimension_values[dim_id]
                    if '..' in time_value:
                        # Range specified
                        parts = time_value.split('..')
                        start_period = parts[0] if parts[0] else None
                        end_period = parts[1] if len(parts) > 1 and parts[1] else None
                    elif time_value and time_value != '*':
                        # Single period - use as both start and end
                        start_period = time_value
                        end_period = time_value
                # Add empty string to maintain dimension count
                key_parts.append('')
            else:
                # Regular dimension
                if dim_id in dimension_values:
                    value = dimension_values[dim_id]
                    # Handle wildcards
                    if value == '*' or value == 'all' or not value:
                        key_parts.append('')  # Empty string for "all values"
                    else:
                        key_parts.append(value)
                else:
                    # Dimension not specified - use empty string
                    key_parts.append('')
        
        # Build the key - maintain all dots even for trailing empty dimensions
        key = '.'.join(key_parts)
        
        # Special case: if all dimensions are empty, use 'all'
        if all(part == '' for part in key_parts):
            key = 'all'
        
        return key, start_period, end_period
    
    def validate_dimension_values(self,
                                 dimension_order: List[str],
                                 dimension_values: Dict[str, str]) -> List[str]:
        """
        Validate that provided dimensions exist in the DSD.
        
        Returns list of validation errors (empty if valid).
        """
        errors = []
        
        # Check for unknown dimensions
        known_dims = set(dimension_order)
        for dim_id in dimension_values:
            if dim_id not in known_dims:
                errors.append(f"Unknown dimension '{dim_id}'. Valid dimensions: {', '.join(dimension_order)}")
        
        # Check for invalid characters in values
        for dim_id, value in dimension_values.items():
            if value and not self._is_valid_key_component(value):
                errors.append(f"Invalid characters in value for dimension '{dim_id}': {value}")
        
        return errors
    
    def _is_valid_key_component(self, value: str) -> bool:
        """Check if a key component contains valid characters."""
        # Based on SDMX regex: ^([\.A-Za-z\d_@$-]+(\+[A-Za-z\d_@$-]+)*)*$
        import re
        # Allow single values or + separated multiple values
        pattern = r'^[A-Za-z\d_@$\-]+(\+[A-Za-z\d_@$\-]+)*$'
        return bool(re.match(pattern, value))
    
    def build_query_url(self,
                       spec: SDMXQuerySpec,
                       dimension_order: List[str]) -> Dict[str, Any]:
        """
        Build a complete SDMX data query URL.
        
        Args:
            spec: Query specification
            dimension_order: Ordered list of dimension IDs from DSD
            
        Returns:
            Dictionary with URL and metadata
        """
        # Validate dimensions
        errors = self.validate_dimension_values(dimension_order, spec.dimension_values)
        if errors:
            return {
                "error": "Validation failed",
                "validation_errors": errors
            }
        
        # Build the key
        key, derived_start, derived_end = self.build_data_key(
            dimension_order,
            spec.dimension_values,
            exclude_time=True
        )
        
        # Use explicitly provided periods or derived ones
        start_period = spec.start_period or derived_start
        end_period = spec.end_period or derived_end
        
        # Build flow parameter: agency,dataflow,version
        flow = f"{spec.agency_id},{spec.dataflow_id},{spec.version}"
        
        # Build base URL
        url = f"{self.base_url}/data/{flow}/{key}/{spec.provider}"
        
        # Add query parameters
        params = []
        
        if start_period:
            params.append(f"startPeriod={start_period}")
        if end_period:
            params.append(f"endPeriod={end_period}")
        
        # Add format parameter
        if spec.format_type.lower() == "csv":
            params.append("format=csv")
        elif spec.format_type.lower() == "json":
            params.append("format=jsondata")
        # Default is structurespecificdata (XML)
        
        if params:
            url += "?" + "&".join(params)
        
        return {
            "url": url,
            "key": key,
            "flow": flow,
            "dimension_order": dimension_order,
            "dimension_values": spec.dimension_values,
            "time_range": {
                "start": start_period,
                "end": end_period
            } if start_period or end_period else None,
            "format": spec.format_type
        }
    
    def explain_key_structure(self,
                             dimension_order: List[str],
                             key: str) -> Dict[str, Any]:
        """
        Explain what a key means based on the dimension order.
        
        Useful for debugging and understanding queries.
        """
        key_parts = key.split('.')
        
        if len(key_parts) != len(dimension_order):
            return {
                "error": f"Key has {len(key_parts)} parts but DSD has {len(dimension_order)} dimensions",
                "key": key,
                "expected_dimensions": dimension_order
            }
        
        explanation = {
            "key": key,
            "dimension_count": len(dimension_order),
            "breakdown": []
        }
        
        for i, (dim_id, value) in enumerate(zip(dimension_order, key_parts)):
            explanation["breakdown"].append({
                "position": i + 1,
                "dimension": dim_id,
                "value": value if value else "all",
                "meaning": "All values" if not value else f"Filter by {value}"
            })
        
        return explanation


def create_example_queries() -> List[Dict[str, Any]]:
    """
    Create example queries demonstrating correct key construction.
    """
    builder = SDMXQueryBuilder()
    
    # Example dimension order from DSD_DIGITAL_DEVELOPMENT
    dimension_order = ["FREQ", "GEO_PICT", "INDICATOR", "TIME_PERIOD"]
    
    examples = []
    
    # Example 1: All data
    spec1 = SDMXQuerySpec(
        dataflow_id="DF_DIGITAL_DEVELOPMENT",
        agency_id="SPC",
        version="1.0",
        dimension_values={},  # No filters
        format_type="csv"
    )
    result1 = builder.build_query_url(spec1, dimension_order)
    result1["description"] = "All data (no filters)"
    examples.append(result1)
    
    # Example 2: Annual data for Tonga, all indicators, year 2020
    spec2 = SDMXQuerySpec(
        dataflow_id="DF_DIGITAL_DEVELOPMENT",
        agency_id="SPC",
        version="1.0",
        dimension_values={
            "FREQ": "A",
            "GEO_PICT": "TO",
            "TIME_PERIOD": "2020"
        },
        format_type="csv"
    )
    result2 = builder.build_query_url(spec2, dimension_order)
    result2["description"] = "Annual data for Tonga in 2020"
    examples.append(result2)
    
    # Example 3: All frequencies, Tonga, specific indicator, time range
    spec3 = SDMXQuerySpec(
        dataflow_id="DF_DIGITAL_DEVELOPMENT",
        agency_id="SPC",
        version="1.0",
        dimension_values={
            "GEO_PICT": "TO",
            "INDICATOR": "DD001"
        },
        start_period="2018",
        end_period="2023",
        format_type="json"
    )
    result3 = builder.build_query_url(spec3, dimension_order)
    result3["description"] = "Specific indicator for Tonga, 2018-2023"
    examples.append(result3)
    
    # Example 4: Multiple countries (using +)
    spec4 = SDMXQuerySpec(
        dataflow_id="DF_DIGITAL_DEVELOPMENT",
        agency_id="SPC",
        version="1.0",
        dimension_values={
            "FREQ": "A",
            "GEO_PICT": "TO+FJ+WS",  # Tonga, Fiji, Samoa
            "INDICATOR": "DD001"
        },
        start_period="2020",
        end_period="2023",
        format_type="csv"
    )
    result4 = builder.build_query_url(spec4, dimension_order)
    result4["description"] = "Annual data for multiple Pacific islands"
    examples.append(result4)
    
    return examples


if __name__ == "__main__":
    # Test the query builder
    print("SDMX Query Builder Examples")
    print("=" * 60)
    
    examples = create_example_queries()
    
    for i, example in enumerate(examples, 1):
        print(f"\nExample {i}: {example.get('description', 'N/A')}")
        print("-" * 40)
        print(f"Key: {example['key']}")
        print(f"URL: {example['url']}")
        if example.get('time_range'):
            print(f"Time: {example['time_range']['start']} to {example['time_range']['end']}")
        
        # Explain the key
        builder = SDMXQueryBuilder()
        explanation = builder.explain_key_structure(
            example['dimension_order'],
            example['key']
        )
        print("\nKey breakdown:")
        for part in explanation.get('breakdown', []):
            print(f"  {part['position']}. {part['dimension']}: {part['meaning']}")