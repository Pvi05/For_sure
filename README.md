# Context

Submission for the 2026 HackEurope hackathon from Paris.

## Briefly explained

LLMs and Agentic Models are going toward a trend of continuous learning. This approach allows for more adaptable models, that adapt and learn on the fly, for exemple to new events. But this also comes with the risk of learning erroneous, wrong or even harmful data, which could cause the Agent to hallucinate in a dangerous way. This is where « for sure ? » comes into play. Our tool ensures that a claim is verified in a reliable and textual way, correcting it if necessary, and therefore always giving the model the correct insight.

## What it does

As a proof of concept, we implemented this fact-checking behaviour by relying on BlueSky posts (cheaper than X…). Given a BlueSky post, the model tries to link it to reliable source. Of course, using an LLM would be ill-suited, as it could of course hallucinate and predict a claim to be correct rather than wrong. Our approach rather gets the best of Gemini’s capabilities by relying rather on its semantics abilities. Using a graph to construct relationships between posts, this ability is used for precise steps along a branch to ensure its semantic continuity. The Graph construction uses strategic pruning based on post reliability score (derived from the user), and virality (derived from engagement). Lastly, when the claim propagation graph is deemed complete, Gemini is called to summarise the claim verifiability and its derivation from sources deemed reliables.

## How we built it

This project was built using Ruby on Rails for the web application, to provide a general access to the API and experimentation with the fact checking model.
The back-end model was built using Python, Gemini’s API for semantics capabilities, a small vector transformation model (paraphrase-multilingual-MiniLM-L12-v2) to compute Pearson’s distance for a first related posts selections.
