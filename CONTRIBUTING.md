# Convention de contribution

## Branches
| Branche | Usage |
|---------|-------|
| `main` | Livrables validés |
| `develop` | Développement en cours |
| `feature/elk-pipeline` | Pipeline Logstash/ES |
| `feature/markov-engine` | Moteur Markov |
| `feature/rag-llm` | Module RAG/LLM |
| `docs/memoire` | Rédaction mémoire |

## Convention de commits
```
type(scope): message en français

feat(markov): ajouter calcul du score d'anomalie par transition
fix(elk): corriger mapping date Kibana dans logstash.conf
docs(memoire): compléter chapitre 4 implémentation technique
config(docker): mettre à jour version ELK vers 9.3.0
```
