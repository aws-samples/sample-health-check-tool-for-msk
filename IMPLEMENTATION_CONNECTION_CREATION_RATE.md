# ConnectionCreationRate - Implementação

## Resumo

Implementada análise da métrica `ConnectionCreationRate` para detectar problemas de reconexão excessiva em clusters MSK, conforme recomendado no documento de Best Practices da AWS.

## Motivação

Segundo o PDF de Best Practices:
- **Novas conexões são caras** (overhead de CPU)
- **IAM auth tem limite de 100 conexões/seg** por cluster
- Alta taxa de criação indica:
  - Falta de connection pooling
  - Timeouts muito curtos
  - Instabilidade de rede
  - Loops de restart de clientes

## Implementação

### 1. Coleta de Métrica (`metrics_collector.py`)
- Adicionada `ConnectionCreationRate` aos dicionários `STANDARD_METRICS` e `EXPRESS_METRICS`
- Métrica per-broker com estatística `Average`
- Total de métricas: **18 → 19** para ambos Standard e Express

### 2. Análise (`analyzer.py`)
Nova função `analyze_connection_creation_rate()` que:

**Calcula estatísticas cluster-wide:**
- Soma das médias de todos os brokers
- P95 (mais importante que picos isolados)
- Máximo

**Thresholds adaptativos:**
- **Com IAM auth:**
  - CRITICAL: P95 ≥ 80 conn/sec (80% do limite de 100/sec)
  - WARNING: P95 ≥ 50 conn/sec (50% do limite)
  
- **Sem IAM auth:**
  - CRITICAL: P95 ≥ 50 conn/sec
  - WARNING: P95 ≥ 20 conn/sec

**Níveis de severidade:**
- **CRITICAL**: P95 acima do threshold crítico
- **WARNING**: P95 acima do threshold de warning
- **INFORMATIONAL**: Média ≥ 5 conn/sec (monitorar)
- **HEALTHY**: Média < 5 conn/sec

### 3. Recomendações (`recommendations.py`)
```
Action: Implementar connection pooling, aumentar timeouts, adicionar exponential backoff com circuit breaker
Rationale: Alta taxa indica falta de pooling, timeouts curtos ou instabilidade
Impact: Aumento de CPU, throttling (IAM), redução de performance
```

### 4. Visualização (`visualizations.py`)
- Título: "Connection Creation Rate"
- Unidade: "Connections/sec"
- Gráfico incluído no PDF report

### 5. PDF Report (`pdf_builder.py`)
Descrição detalhada:
> "Rate of new connections being created per second. High rates indicate missing connection pooling, short timeouts, or client instability. IAM auth is limited to 100 connections/sec."

### 6. Documentação (`README.md`)
- Atualizado de 18 para 19 métricas
- `ConnectionCreationRate` listada para Standard e Express

### 7. Testes (`test_metrics_collector.py`)
- Atualizado para validar presença de `ConnectionCreationRate`
- Teste passa ✅

## Comportamento

### Exemplo 1: Cluster com IAM auth, P95 = 85 conn/sec
```
Severity: CRITICAL
Title: Excessive Connection Creation Rate
Description: High connection creation rate detected: P95=85.0 conn/sec, 
avg=45.0 conn/sec, max=120.0 conn/sec (approaching IAM auth limit of 100/sec). 
This indicates missing connection pooling, short timeouts, or client instability. 
New connections are expensive and impact CPU performance.
```

### Exemplo 2: Cluster sem IAM auth, P95 = 25 conn/sec
```
Severity: WARNING
Title: Elevated Connection Creation Rate
Description: Elevated connection creation rate: P95=25.0 conn/sec, 
avg=15.0 conn/sec, max=40.0 conn/sec. Consider implementing connection 
pooling and reviewing client timeout configurations.
```

### Exemplo 3: Cluster saudável, P95 = 2 conn/sec
```
Severity: HEALTHY
Title: Low Connection Creation Rate
Description: Connection creation rate is low: P95=2.0 conn/sec, 
avg=1.5 conn/sec. This indicates stable client connections.
```

## Diferencial vs Métrica Existente

A ferramenta já tinha `ConnectionCount` (total de conexões), mas **não** `ConnectionCreationRate`:

| Métrica | O que mede | Problema detectado |
|---------|------------|-------------------|
| `ConnectionCount` | Total de conexões ativas | Esgotamento de conexões |
| `ConnectionCreationRate` | **Taxa de novas conexões/seg** | **Churn excessivo, falta de pooling** |

## Arquivos Modificados

1. ✅ `msk_health_check/metrics_collector.py` - Coleta
2. ✅ `msk_health_check/analyzer.py` - Análise
3. ✅ `msk_health_check/recommendations.py` - Recomendações
4. ✅ `msk_health_check/visualizations.py` - Gráficos
5. ✅ `msk_health_check/pdf_builder.py` - PDF
6. ✅ `README.md` - Documentação
7. ✅ `tests/test_metrics_collector.py` - Testes

## Próximos Passos

Para testar em um cluster real:
```bash
msk-health-check \
  --region us-east-1 \
  --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid \
  --debug
```

O report PDF agora incluirá:
- Gráfico de `ConnectionCreationRate` ao longo do tempo
- Finding com análise (se aplicável)
- Recomendação priorizada (se CRITICAL/WARNING)
