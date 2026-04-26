# PBI-Ixtractor Improvement Plan

## Current Functionality

### What It Does
PBI-Ixtractor is a Power BI documentation tool that:
1. **Extracts PBIX metadata** - Treats .pbix files as ZIP archives and parses Report/Layout JSON
2. **Integrates with Tabular Editor 2** - Generates TSV files with measure/column definitions and DAX formulas
3. **Generates Excel documentation** with:
   - Color-coded DAX formulas (functions, measures, variables, comments)
   - Relationship diagrams (NetworkX graphs)
   - Visual inventory per page (charts, slicers, buttons, groups)
   - Filter documentation (visual-level and page-level)
   - Unused columns/measures report
   - Dependency tracking (which measures use which columns)

### Architecture (Current)
- **Single monolithic class** (`ReportExtractor`) with ~700 lines
- **Hardcoded visual type handling** - Each visual type has custom parsing logic
- **CSV-based configuration** - Visual types, data types, functions loaded from CSV files
- **Two-phase extraction**:
  1. Parse PBIX JSON layout (visuals, filters, buttons)
  2. Parse Tabular Editor TSV (measures, columns, DAX)
- **XlsxWriter output** - Rich text formatting for DAX syntax highlighting

### Known Issues (from ReadMe.txt)
- Buttons/bookmarks not always connected correctly
- Hierarchies sometimes missed
- Non-visual elements (shapes, images) inconsistently documented

---

## Proposed Improvements

### 1. DevOps & .pbir Format Support

#### Azure DevOps Integration
**Goal**: Extract Power BI reports from Azure DevOps Git repositories, bypassing sensitivity label restrictions.

**Implementation**:
```python
# New module: devops_client.py
class DevOpsClient:
    """Azure DevOps API client for Power BI reports"""
    
    def __init__(self, organization: str, project: str, pat_token: str):
        self.base_url = f"https://dev.azure.com/{organization}/{project}/_apis"
        self.auth = ("", pat_token)
    
    def list_repositories(self) -> List[str]:
        """List all Git repositories in the project"""
        ...
    
    def get_pbir_files(self, repo_name: str, branch: str = "main") -> List[str]:
        """Find all .pbir definition files in repository"""
        ...
    
    def download_report(self, repo: str, path: str, output_dir: str):
        """Download .pbir folder structure from DevOps"""
        ...
```

#### .pbir Format Support
**Goal**: Support the new Git-friendly .pbir format (folder structure with JSON files).

**Structure**:
```
MyReport.pbir/
├── definition.pbir           # Main report definition (JSON)
├── report.json               # Layout/visuals (similar to Report/Layout)
├── localSettings.json        # User settings
├── .pbi/
│   └── localSettings.json
└── *.Dataset/
    └── model.bim             # Tabular model (replaces .bim extraction)
```

**Implementation**:
```python
# New module: pbir_parser.py
class PBIRExtractor:
    """Parser for .pbir format (DevOps-friendly structure)"""
    
    def __init__(self, pbir_path: str):
        self.pbir_path = Path(pbir_path)
        self.report_json = self.pbir_path / "report.json"
        self.bim_path = self._find_bim_file()
    
    def _find_bim_file(self) -> Path:
        """Locate model.bim in *.Dataset folder"""
        dataset_dirs = list(self.pbir_path.glob("*.Dataset"))
        return dataset_dirs[0] / "model.bim" if dataset_dirs else None
    
    def extract(self) -> Dict:
        """Extract report metadata (similar to ReportExtractor)"""
        with open(self.report_json, 'r', encoding='utf-8') as f:
            return json.load(f)
```

**Benefits**:
- Works with sensitivity-labeled reports in DevOps
- No need to unzip .pbix files
- Easier version control integration
- Direct model.bim access (no Tabular Editor required for extraction)

---

### 2. Improved JSON Extraction

#### Problems with Current Approach
- String manipulation (`json.loads()` on embedded strings)
- Deep nested dictionary traversal (`find_value_by_key` is recursive and slow)
- No schema validation
- Hardcoded key paths

#### Proposed Solution: Pydantic Models + JSONPath

```python
# New module: pbi_models.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import jsonpath_ng as jp

class VisualProjection(BaseModel):
    """Represents a single field in a visual"""
    queryRef: str
    displayName: Optional[str] = None
    active: Optional[bool] = True

class VisualConfig(BaseModel):
    """Parsed visual configuration"""
    name: str
    visualType: str
    projections: Dict[str, List[VisualProjection]]
    filters: List[Dict] = Field(default_factory=list)
    
class ReportSection(BaseModel):
    """Page in Power BI report"""
    name: str
    displayName: str
    visualContainers: List[Dict]
    filters: str  # JSON string
    
class ReportLayout(BaseModel):
    """Root layout document"""
    sections: List[ReportSection]
    config: str  # JSON string
```

**JSONPath Queries** (replace recursive traversal):
```python
from jsonpath_ng.ext import parse

# Find all visual types
visual_type_path = parse("$.sections[*].visualContainers[*].config.singleVisual.visualType")
matches = [match.value for match in visual_type_path.find(layout)]

# Find all filter expressions
filter_path = parse("$.sections[*].visualContainers[*].filters[*].filter.Where[*]")
```

**Benefits**:
- Type safety with Pydantic
- 10-100x faster than recursive dict traversal
- Easier to maintain (schema-driven)
- Better error handling

---

### 3. Module-Based Visual Type Architecture

#### Current Problem
Visual types are hardcoded in a giant if/elif chain (lines 320-500). Adding new visual types requires code changes.

#### Proposed Plugin Architecture

```python
# New module: visual_plugins/base.py
from abc import ABC, abstractmethod
from typing import List, Tuple

class VisualPlugin(ABC):
    """Base class for visual type extractors"""
    
    @property
    @abstractmethod
    def visual_type(self) -> str:
        """Power BI visual type identifier"""
        pass
    
    @abstractmethod
    def extract_fields(self, config: Dict, projections: Dict) -> List[Tuple]:
        """
        Extract fields from visual configuration.
        Returns: [(table, column, display_name, data_type), ...]
        """
        pass
    
    @abstractmethod
    def extract_properties(self, config: Dict) -> Dict:
        """Extract visual-specific properties (titles, format, etc.)"""
        pass
```

**Example Plugin Implementation**:
```python
# visual_plugins/clustered_column.py
class ClusteredColumnPlugin(VisualPlugin):
    visual_type = "clusteredColumnChart"
    
    def extract_fields(self, config: Dict, projections: Dict) -> List[Tuple]:
        fields = []
        
        # X-axis (Category)
        for item in projections.get("Category", []):
            table, column = self._parse_query_ref(item["queryRef"])
            fields.append((table, column, item.get("displayName"), "Categorical"))
        
        # Y-axis (Values)
        for item in projections.get("Y", []):
            table, column = self._parse_query_ref(item["queryRef"])
            fields.append((table, column, item.get("displayName"), "Y-Values"))
        
        return fields
    
    def extract_properties(self, config: Dict) -> Dict:
        return {
            "title": config.get("title", {}).get("text", ""),
            "legend_position": config.get("legend", {}).get("position", "Right")
        }
```

**Plugin Registry**:
```python
# visual_plugins/__init__.py
class VisualPluginRegistry:
    """Dynamically discover and register visual plugins"""
    
    def __init__(self):
        self._plugins: Dict[str, VisualPlugin] = {}
        self._discover_plugins()
    
    def _discover_plugins(self):
        """Auto-discover all plugin classes in visual_plugins/"""
        import importlib
        import pkgutil
        
        for _, module_name, _ in pkgutil.iter_modules([Path(__file__).parent]):
            if module_name == "base":
                continue
            
            module = importlib.import_module(f"visual_plugins.{module_name}")
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, type) and issubclass(attr, VisualPlugin) and attr != VisualPlugin:
                    plugin = attr()
                    self._plugins[plugin.visual_type] = plugin
    
    def get_plugin(self, visual_type: str) -> Optional[VisualPlugin]:
        return self._plugins.get(visual_type)
    
    def supported_types(self) -> List[str]:
        return list(self._plugins.keys())
```

**Usage**:
```python
# In ReportExtractor
registry = VisualPluginRegistry()

for container in section["visualContainers"]:
    visual_type = container["config"]["singleVisual"]["visualType"]
    
    plugin = registry.get_plugin(visual_type)
    if plugin:
        fields = plugin.extract_fields(container["config"], container["query"]["projections"])
        properties = plugin.extract_properties(container["config"])
    else:
        log.warning(f"Unsupported visual type: {visual_type}")
```

**Benefits**:
- Easy to add new visual types (drop a file in `visual_plugins/`)
- No more CSV maintenance
- Each visual isolated to ~50 lines
- Community-extensible

---

### 4. Better Output Format

#### Current Issues
- Excel-only output (not machine-readable)
- DAX formatting uses manual token parsing
- No summary statistics
- Hard to compare versions

#### Proposed Multi-Format Output

**A. Interactive HTML Report** (Primary Output)
```python
# New module: output/html_generator.py
from jinja2 import Template
import plotly.graph_objects as go

class HTMLReportGenerator:
    """Generate interactive HTML documentation"""
    
    def generate(self, report_data: ReportData, output_path: str):
        template = self._load_template()
        
        html = template.render(
            report_name=report_data.name,
            pages=self._generate_page_tabs(report_data.pages),
            measures=self._generate_measure_cards(report_data.measures),
            relationships=self._generate_interactive_graph(report_data.relationships),
            filters=report_data.filters,
            summary=self._generate_summary(report_data)
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
    
    def _generate_interactive_graph(self, relationships: List) -> str:
        """Use Plotly for interactive relationship diagram"""
        import networkx as nx
        
        G = nx.DiGraph()
        # ... build graph ...
        
        fig = go.Figure(data=[
            go.Scatter(x=x_edges, y=y_edges, mode='lines'),
            go.Scatter(x=x_nodes, y=y_nodes, mode='markers+text', text=labels)
        ])
        
        return fig.to_html(include_plotlyjs='cdn')
```

**Features**:
- Collapsible sections per page
- Search/filter measures
- Interactive relationship graph (zoom, pan, click)
- DAX syntax highlighting via Prism.js
- Dark mode support

**B. JSON Export** (Machine-Readable)
```python
# New module: output/json_exporter.py
class JSONExporter:
    """Export to structured JSON for API consumption"""
    
    def export(self, report_data: ReportData, output_path: str):
        output = {
            "metadata": {
                "report_name": report_data.name,
                "extracted_at": datetime.now().isoformat(),
                "extractor_version": __version__
            },
            "measures": [
                {
                    "name": m.name,
                    "table": m.table,
                    "dax": m.definition,
                    "dependencies": m.dependencies,
                    "used_in_pages": m.pages,
                    "format_string": m.format_string
                }
                for m in report_data.measures
            ],
            "pages": [...],
            "relationships": [...],
            "unused_items": report_data.unused_columns
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2)
```

**C. Markdown Export** (Git-Friendly)
```python
# New module: output/markdown_exporter.py
class MarkdownExporter:
    """Generate Markdown documentation for Git repos"""
    
    def export(self, report_data: ReportData, output_dir: Path):
        # Create one .md file per page
        for page in report_data.pages:
            md_content = f"# {page.name}\n\n"
            
            # Visual inventory
            md_content += "## Visuals\n\n"
            for visual in page.visuals:
                md_content += f"### {visual.name} ({visual.type})\n"
                md_content += f"**Fields:**\n"
                for field in visual.fields:
                    md_content += f"- `{field.table}[{field.column}]`\n"
            
            # Filters
            md_content += "\n## Page Filters\n\n"
            # ...
            
            (output_dir / f"{page.name}.md").write_text(md_content, encoding='utf-8')
        
        # Create index file
        self._generate_index(report_data, output_dir)
```

**Output Structure**:
```
MyReport_Documentation/
├── index.html              # Interactive report
├── data.json               # Machine-readable export
├── MyReport.xlsx           # Excel (legacy format)
├── markdown/
│   ├── README.md           # Overview
│   ├── Page1.md
│   ├── Page2.md
│   └── measures.md
└── images/
    └── relationships.png
```

---

### 5. Nice-to-Have Features

#### A. Unused Measures Detection
```python
# In analyzer.py
class MeasureAnalyzer:
    """Analyze measure usage and quality"""
    
    def find_unused_measures(self, measures: List, visuals: List) -> List:
        """Find measures not used in any visual"""
        used_measures = set()
        
        for visual in visuals:
            for field in visual.fields:
                if field.type == "Measure":
                    used_measures.add((field.table, field.name))
        
        all_measures = {(m.table, m.name) for m in measures}
        unused = all_measures - used_measures
        
        # Check if unused measures are dependencies
        dependency_graph = self._build_dependency_graph(measures)
        truly_unused = []
        
        for measure in unused:
            # If no other measure depends on this one
            if not any(measure in deps for deps in dependency_graph.values()):
                truly_unused.append(measure)
        
        return truly_unused
```

#### B. Similar Measures Detection
```python
from difflib import SequenceMatcher
from Levenshtein import distance  # pip install python-Levenshtein

class MeasureAnalyzer:
    
    def find_similar_measures(self, measures: List, threshold: float = 0.85) -> List[Tuple]:
        """
        Find measures with similar DAX definitions.
        Returns: [(measure1, measure2, similarity_score), ...]
        """
        similar_pairs = []
        
        for i, m1 in enumerate(measures):
            for m2 in measures[i+1:]:
                # Normalize DAX (remove whitespace, comments)
                dax1 = self._normalize_dax(m1.definition)
                dax2 = self._normalize_dax(m2.definition)
                
                # Calculate similarity
                similarity = SequenceMatcher(None, dax1, dax2).ratio()
                
                if similarity >= threshold:
                    similar_pairs.append((
                        f"{m1.table}[{m1.name}]",
                        f"{m2.table}[{m2.name}]",
                        similarity
                    ))
        
        return sorted(similar_pairs, key=lambda x: x[2], reverse=True)
    
    def _normalize_dax(self, dax: str) -> str:
        """Remove whitespace and comments for comparison"""
        # Remove comments
        dax = re.sub(r'//.*?\n', '', dax)
        # Remove extra whitespace
        dax = ' '.join(dax.split())
        return dax.lower()
```

#### C. DAX Complexity Metrics
```python
class DAXMetrics:
    """Calculate DAX complexity scores"""
    
    def calculate_complexity(self, dax: str) -> Dict:
        """
        Calculate various complexity metrics.
        Returns: {
            "lines": int,
            "variables": int,
            "nesting_depth": int,
            "function_calls": int,
            "cyclomatic_complexity": int
        }
        """
        
        return {
            "lines": len(dax.split('\n')),
            "variables": dax.count("VAR "),
            "nesting_depth": self._max_nesting_depth(dax),
            "function_calls": len(re.findall(r'\b[A-Z]+\s*\(', dax)),
            "cyclomatic_complexity": self._calculate_cyclomatic(dax)
        }
    
    def _max_nesting_depth(self, dax: str) -> int:
        """Calculate maximum parenthesis nesting depth"""
        max_depth = 0
        current_depth = 0
        
        for char in dax:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth -= 1
        
        return max_depth
```

#### D. Dependency Graph Visualization
```python
# Enhanced relationship viewer
class DependencyVisualizer:
    """Create interactive dependency graphs"""
    
    def create_measure_dependency_graph(self, measures: List) -> str:
        """
        Create Plotly network graph showing measure dependencies.
        Returns HTML string with interactive graph.
        """
        import plotly.graph_objects as go
        import networkx as nx
        
        G = nx.DiGraph()
        
        # Build graph
        for measure in measures:
            G.add_node(f"{measure.table}[{measure.name}]", 
                      table=measure.table,
                      complexity=len(measure.definition))
            
            for dep in measure.dependencies:
                G.add_edge(dep, f"{measure.table}[{measure.name}]")
        
        # Layout
        pos = nx.spring_layout(G, k=1.5, iterations=100)
        
        # Create edges
        edge_traces = []
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_traces.append(
                go.Scatter(x=[x0, x1, None], y=[y0, y1, None],
                          mode='lines', line=dict(width=1, color='#888'))
            )
        
        # Create nodes
        node_x = [pos[node][0] for node in G.nodes()]
        node_y = [pos[node][1] for node in G.nodes()]
        
        node_trace = go.Scatter(
            x=node_x, y=node_y, mode='markers+text',
            text=[node for node in G.nodes()],
            textposition="top center",
            marker=dict(size=10, color='lightblue')
        )
        
        fig = go.Figure(data=edge_traces + [node_trace])
        fig.update_layout(showlegend=False, hovermode='closest')
        
        return fig.to_html(include_plotlyjs='cdn')
```

#### E. Change Detection (Compare Reports)
```python
class ReportComparer:
    """Compare two versions of a report"""
    
    def compare(self, report1: ReportData, report2: ReportData) -> Dict:
        """
        Generate diff between two report versions.
        Returns: {
            "new_measures": [...],
            "deleted_measures": [...],
            "modified_measures": [...],
            "new_pages": [...],
            "deleted_pages": [...]
        }
        """
        
        measures1 = {(m.table, m.name): m for m in report1.measures}
        measures2 = {(m.table, m.name): m for m in report2.measures}
        
        return {
            "new_measures": list(set(measures2.keys()) - set(measures1.keys())),
            "deleted_measures": list(set(measures1.keys()) - set(measures2.keys())),
            "modified_measures": [
                key for key in measures1.keys() & measures2.keys()
                if measures1[key].definition != measures2[key].definition
            ],
            "new_pages": list(set(report2.pages) - set(report1.pages)),
            "deleted_pages": list(set(report1.pages) - set(report2.pages))
        }
```

---

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)
- [ ] Create modular project structure
- [ ] Implement Pydantic models for PBI objects
- [ ] Replace recursive traversal with JSONPath
- [ ] Unit tests for JSON extraction

### Phase 2: Plugin Architecture (Weeks 3-4)
- [ ] Build VisualPlugin base class
- [ ] Create plugin registry with auto-discovery
- [ ] Migrate top 10 visual types to plugins
- [ ] Add plugin template generator

### Phase 3: DevOps Integration (Week 5)
- [ ] Implement DevOpsClient
- [ ] Add .pbir format support
- [ ] Test with sensitivity-labeled reports
- [ ] Add CLI flags for DevOps auth

### Phase 4: Output Improvements (Weeks 6-7)
- [ ] JSON exporter
- [ ] Markdown exporter
- [ ] HTML generator with Jinja2 templates
- [ ] Interactive Plotly graphs

### Phase 5: Advanced Features (Weeks 8-9)
- [ ] Unused measure detection
- [ ] Similar measure detection
- [ ] DAX complexity metrics
- [ ] Dependency graph visualization

### Phase 6: Polish (Week 10)
- [ ] Documentation
- [ ] Performance optimization
- [ ] Error handling improvements
- [ ] Package for distribution

---

## Technology Stack Updates

### New Dependencies
```toml
[project]
dependencies = [
    # Existing
    "pandas>=2.0.0",
    "xlsxwriter>=3.2.9",
    "matplotlib>=3.10.0",
    "networkx>=3.6.0",
    
    # New
    "pydantic>=2.0.0",           # Schema validation
    "jsonpath-ng>=1.6.0",        # JSON querying
    "jinja2>=3.1.0",             # HTML templates
    "plotly>=5.18.0",            # Interactive graphs
    "python-Levenshtein>=0.25.0", # String similarity
    "azure-devops>=7.1.0",       # DevOps API
    "requests>=2.31.0",          # HTTP client
    "pygments>=2.17.0",          # DAX syntax highlighting
    "click>=8.1.0",              # Better CLI
]
```

### Project Structure
```
PBIxtractor/
├── pbixtractor/
│   ├── __init__.py
│   ├── cli.py                  # Click-based CLI
│   ├── extractors/
│   │   ├── pbix_extractor.py   # Legacy .pbix
│   │   ├── pbir_extractor.py   # New .pbir
│   │   └── devops_client.py    # DevOps integration
│   ├── models/
│   │   ├── report.py           # Pydantic models
│   │   ├── visual.py
│   │   └── measure.py
│   ├── visual_plugins/
│   │   ├── base.py
│   │   ├── clustered_column.py
│   │   ├── slicer.py
│   │   └── ...
│   ├── analyzers/
│   │   ├── measure_analyzer.py
│   │   ├── dependency_analyzer.py
│   │   └── dax_metrics.py
│   ├── outputs/
│   │   ├── excel_generator.py
│   │   ├── html_generator.py
│   │   ├── json_exporter.py
│   │   └── markdown_exporter.py
│   └── utils/
│       ├── dax_parser.py
│       └── json_utils.py
├── tests/
│   ├── test_extractors.py
│   ├── test_plugins.py
│   └── fixtures/
├── templates/
│   ├── report.html.j2
│   └── page.html.j2
├── docs/
├── pyproject.toml
└── README.md
```

---

## Breaking Changes & Migration

### CLI Changes
**Old:**
```bash
python PB-Ixtractor.py --ui
```

**New:**
```bash
# Interactive UI (unchanged)
pbixtractor ui

# CLI mode with more options
pbixtractor extract report.pbix --output-dir ./docs --format html,json,excel

# DevOps mode
pbixtractor extract-devops \
    --org MyOrg \
    --project MyProject \
    --repo Reports \
    --path MyReport.pbir \
    --pat-token $DEVOPS_PAT
```

### Configuration File
Replace CSV files with single YAML config:
```yaml
# config.yaml
visual_plugins:
  enabled: auto  # or list specific plugins
  
output_formats:
  - html
  - json
  - markdown
  
analyzer:
  detect_unused_measures: true
  similarity_threshold: 0.85
  complexity_metrics: true
  
devops:
  organization: "MyOrg"
  project: "MyProject"
  # pat_token from environment variable
```

---

## Questions for Consideration

1. **Tabular Editor Dependency**: Should we keep TE2 requirement or reimplement .bim parsing?
   - Pro keeping: TE2 is battle-tested, handles complex models
   - Pro removing: Better cross-platform support, no external dependencies

2. **UI Framework**: Keep Dear PyGui or switch to web-based (e.g., Streamlit, Gradio)?
   - Dear PyGui: Native, lightweight
   - Web UI: Easier deployment, better for teams

3. **Caching Strategy**: Cache extracted data to speed up re-runs?
   - SQLite cache for measure definitions
   - Redis for DevOps API responses

4. **Async Processing**: Use async/await for DevOps API + file I/O?
   - Would speed up batch processing significantly

Let me know which features you'd like to prioritize!
