# AI Bot User Agents Reference

This document lists official documentation sources for AI crawler user agents and their classification for the LLM bot traffic pipeline.

## Official Documentation Sources

### OpenAI
- **Official Docs**: https://platform.openai.com/docs/bots
- **Bots**:
  | User Agent | Purpose | Category |
  |------------|---------|----------|
  | `GPTBot` | AI training data collection for GPT models (ChatGPT, GPT-4o) | `training` |
  | `ChatGPT-User` | Real-time web browsing when users interact with ChatGPT | `user_request` |
  | `OAI-SearchBot` | AI search indexing for ChatGPT search features (not for training) | `search` |

### Anthropic
- **Official Docs**: https://www.anthropic.com/robots-txt (inferred)
- **Bots**:
  | User Agent | Purpose | Category |
  |------------|---------|----------|
  | `ClaudeBot` | AI training data collection for Claude models | `training` |
  | `Claude-User` | Real-time web access when Claude users browse | `user_request` |
  | `Claude-SearchBot` | AI search indexing for Claude search capabilities | `search` |

### Perplexity
- **Official Docs**: https://docs.perplexity.ai/guides/bots
- **IP Ranges**: 
  - PerplexityBot: https://www.perplexity.com/perplexitybot.json
  - Perplexity-User: https://www.perplexity.com/perplexity-user.json
- **Bots**:
  | User Agent | Purpose | Category |
  |------------|---------|----------|
  | `PerplexityBot` | AI search indexing for Perplexity's answer engine (not for AI training) | `search` |
  | `Perplexity-User` | Real-time browsing when Perplexity users request information | `user_request` |

### Google
- **Official Docs**: https://developers.google.com/search/docs/crawling-indexing/overview-google-crawlers
- **Bots**:
  | User Agent | Purpose | Category |
  |------------|---------|----------|
  | `Googlebot` | Primary crawler for Google Search indexing | `search` |
  | `Google-Extended` | Token controlling AI training usage of Googlebot-crawled content | `training` |
  | `Gemini-Deep-Research` | AI research agent for Google Gemini's Deep Research feature | `user_request` |
  | `Google-CloudVertexBot` | AI agent for Vertex AI Agent Builder | `user_request` |

### Microsoft
- **Official Docs**: https://www.bing.com/webmaster/help/which-crawlers-does-bing-use-8c184ec0
- **Bots**:
  | User Agent | Purpose | Category |
  |------------|---------|----------|
  | `bingbot` | Powers Bing Search and Bing Chat (Copilot) AI answers | `search` |

### Apple
- **Official Docs**: https://support.apple.com/en-us/HT204683
- **Bots**:
  | User Agent | Purpose | Category |
  |------------|---------|----------|
  | `Applebot-Extended` | Controls how Apple uses Applebot data for AI training | `training` |

### Meta
- **Official Docs**: https://developers.facebook.com/docs/sharing/webmasters/crawler
- **Bots**:
  | User Agent | Purpose | Category |
  |------------|---------|----------|
  | `Meta-ExternalAgent` | AI training data collection for Meta's LLMs (Llama, etc.) | `training` |
  | `Meta-WebIndexer` | Used to improve Meta AI search | `search` |

### Other AI Crawlers
| User Agent | Provider | Purpose | Category |
|------------|----------|---------|----------|
| `Amazonbot` | Amazon | AI training for Alexa and other Amazon AI services | `training` |
| `DuckAssistBot` | DuckDuckGo | AI search indexing for DuckDuckGo | `search` |
| `MistralAI-User` | Mistral | Real-time citation fetcher for "Le Chat" assistant | `user_request` |
| `Bytespider` | ByteDance | AI training data for TikTok LLMs | `training` |
| `CCBot` | Common Crawl | Open-source web archive used as training data | `training` |
| `Diffbot` | Diffbot | Data extraction for AI companies | `training` |

## Bot Categories

Our pipeline classifies bots into three categories:

| Category | Description | Session Analysis |
|----------|-------------|------------------|
| `training` | Crawlers collecting data for AI model training | Excluded |
| `user_request` | Real-time browsing triggered by user queries | Included |
| `search` | Search indexing bots — AI-powered and traditional (not training, not real-time user) | Excluded |

**Query Fan-Out Session Analysis**: Only `user_request` bots are included because they represent actual user interactions that trigger web browsing, making their traffic patterns meaningful for understanding LLM query fan-out behavior.

## Comprehensive Reference

For a complete, up-to-date list of AI crawlers, see:
- https://www.searchenginejournal.com/ai-crawler-user-agents-list/558130/

## Full User Agent Strings

| Bot Name | Full User Agent String |
|----------|------------------------|
| Googlebot | `Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)` |
| GPTBot | `Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.3; +https://openai.com/gptbot)` |
| ChatGPT-User | `Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ChatGPT-User/1.0; +https://openai.com/bot` |
| OAI-SearchBot | `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36; compatible; OAI-SearchBot/1.3; +https://openai.com/searchbot` |
| ClaudeBot | `Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)` |
| Claude-User | `Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Claude-User/1.0; +Claude-User@anthropic.com)` |
| PerplexityBot | `Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; PerplexityBot/1.0; +https://perplexity.ai/perplexitybot)` |
| Perplexity-User | `Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Perplexity-User/1.0; +https://perplexity.ai/perplexity-user)` |
| bingbot | `Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) Chrome/116.0.1938.76 Safari/537.36` |

## Last Updated

2026-01-22
