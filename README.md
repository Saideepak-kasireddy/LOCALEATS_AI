# 🍽️ LocEats: Multi-Agent LLM Restaurant Discovery Platform
An intelligent multi-agent system for personalized restaurant recommendations using collaborative LLM agents and real-time data integration.

This system demonstrates how specialized AI agents work together like a personal dining concierge understanding preferences, analyzing locations, processing reviews, and delivering context-aware recommendations with natural language interaction.


## 🎯 Project Goal
Traditional restaurant discovery apps fail to provide truly personalized, context-aware recommendations.

Inspired by advanced multi-agent architectures and real-world impact requirements, this project aims to:

✅ **Automate personalized dining discovery** through natural conversation  
✅ **Handle complex group dining scenarios** with conflicting preferences  
✅ **Integrate real-time context** (weather, events, traffic)  
✅ **Reduce decision time** from 20+ minutes to instant recommendations  

## 🧠 System Architecture
```
User Natural Language Query
         ↓
[Orchestrator Agent] → Routes queries and manages agent collaboration
         ↓
[Review Agent] → Analyzes sentiment, extracts insights from reviews
         ↓
[Context Agent] → Integrates weather, events, time-of-day factors
         ↓
[Decision Agent] → Ranks options and generates explanations
         ↓
[Chat Interface] → Delivers personalized recommendations
```


## 📂 Tech Stack
| Component | Tool |
|-----------|------|
| **UI** | Streamlit / React Chat Interface |
| **LLM Framework** | LangChain / LlamaIndex |
| **Base Models** | GPT-4 / Claude / Llama2 |
| **Vector Database** | Pinecone / Snwoflake |
| **Data Transformation** | dbt Core |
| **Database** | Snowflake|

## 📊 Dataset
The system integrates multiple real-time and static data sources:

```
data/
├── restaurants/           # Google Places, Yelp API data
├── reviews/              # Aggregated review data
├── user_preferences/     # Historical user interactions
├── contextual/          # Weather, events, traffic
└── embeddings/          # Vector representations
```

## 💡 Key Features

🔹 **Natural Language Understanding** – Chat naturally about dining preferences  
🔹 **Multi-Agent Collaboration** – 4 specialized agents working in harmony  
🔹 **Group Consensus** – Handles multiple users with different restrictions  
🔹 **Real-Time Context** – Weather, traffic, events influence recommendations  

## 📈 Example Use Cases

**Solo Dining:**
> "I'm vegetarian and want something cozy for this rainy evening under $20"

**Group Coordination:**
> "Find a restaurant for 6 people - 2 vegans, 1 gluten-free, near downtown for tomorrow's lunch"

**Context-Aware:**
> "What's good near the concert venue after the show ends at 10pm?"

**Discovery Mode:**
> "Surprise me with a hidden gem I haven't tried, authentic Asian cuisine"

## 🔬 Multi-Agent Deep Dive

### Agent Specializations:

**🎯 Orchestrator Agent**
- Routes queries to appropriate agents
- Manages inter-agent communication
- Synthesizes final recommendations

**⭐ Review Agent**
- NLP-based review analysis
- Sentiment extraction
- Trend identification

**🌤️ Context Agent**
- Weather API integration
- Event calendar checking
- Time-based recommendations

**🎯 Decision Agent**
- Multi-criteria ranking
- Explanation generation
- Confidence scoring
``` 
## 📄 License

This project is developed as part of academic coursework at Northeastern University.
