# Goshawk Breeding Habitat Analysis

This repository contains a Python-based geospatial analysis workflow for developing regional **breeding-season habitat suitability maps for Northern Goshawk**. The outputs are intended to support forest management, conservation planning, and statutory decision-making under the *Forest and Range Practices Act*.

The analysis focuses on identifying landscapes capable of supporting **stable goshawk occupancy** by evaluating the spatial relationship between nesting habitat and foraging habitat.

---

## Project Goals

The workflow supports the following objectives:

1. Identify potential goshawk nesting areas for inventory and planning  
2. Quantify changes in breeding habitat potential over time (cumulative effects)  
3. Support targets for minimum habitat thresholds or maximum disturbance (LUP / FLP objectives)  
4. Identify high-value landscape units suitable for Government Actions Regulation (GAR) orders  
5. Support post-GAR field surveys, monitoring, and evaluation  

---

## Conceptual Approach

Sustainable goshawk populations require **both nesting habitat (~200 ha patches)** and **sufficient surrounding foraging habitat (~2,000–2,400 ha)** in functional proximity.

This project implements a streamlined, repeatable workflow that:

- Maps existing nesting and foraging habitat independently  
- Identifies patches of sufficient size and configuration to support occupancy  
- Integrates these layers using focal and overlap analyses to produce a high-resolution breeding habitat suitability surface  
- Enables comparison across time periods to assess trends in habitat supply and risk to persistence  

Analyses are performed at a **30 × 30 m resolution** and are designed to be repeatable across multiple Timber Supply Areas (TSAs).

---

## Study Areas

Initial analyses will be completed for the following Omineca TSAs:

- Robson Valley  
- Prince George  
- Mackenzie  

Where capacity permits, analyses will be repeated using forest inventory from approximately **2000** and **2025** to estimate temporal change resulting from natural disturbance and forest management.

---

## Data Requirements

Key spatial datasets include:

- Biogeoclimatic Ecosystem Classification (BEC) zones  
- Forest structural stage  
- Vegetation Resource Inventory (VRI)  
- Harvested Areas of BC (RESULTS + Consolidated Cutblocks)  
- BC Cumulative Effects Framework – Human Disturbance (internal)  
- Historical wildfire perimeters  
- Landscape Units of British Columbia  

> **Note:** Some datasets are internally available and are not distributed with this repository.

---

## Analytical Workflow

### Step 1a: Nesting Habitat Raster

- Select mature and old forest (>100 years; age class 6+)  
- Apply structural filters (height ≥ 19.5 m, crown closure ≥ 26%)  
- Exclude low productivity stands (site index < 10) and fir parkland BEC zones  
- Remove harvested areas and recent wildfire (<80 years; severity-filtered if available)  
- Rasterize to 30 m resolution (binary)

---

### Step 1b: Breeding-Season Foraging Raster

- Select all forested areas >40 years old  
- Remove harvested areas and recent wildfire (<40 years)  
- Identify high-quality foraging habitat (>80 years)  
- Rasterize to 30 m resolution (ternary classification)

---

### Step 2a: Nesting Patch Identification

- Identify contiguous nesting habitat patches ≥200 ha  
- Produce binary raster indicating qualifying nesting patches  

---

### Step 2b: Focal Foraging Analysis

- Apply a moving window (radius = 2.764 km; ~2,400 ha)  
- Calculate proportion of high-quality foraging habitat (>80 years)  
- Classify areas exceeding a 55% foraging threshold  

---

### Final Breeding Habitat Layer

- Identify spatial overlap between nesting patches and focal foraging areas  
- Output a binary raster representing functional breeding habitat  
- Produce a high-resolution heat map ranking relative habitat contribution  

---

## Repository Structure

```text
src/          Core analysis logic (raster, vector, focal analyses)
scripts/      Workflow entry points and orchestration
data/         Raw, interim, and processed spatial data (not versioned)
outputs/      Maps, tables, and derived products
docs/         Methodology, assumptions, and references
tests/        Unit tests for core logic
