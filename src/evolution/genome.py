from dataclasses import dataclass
import random
import copy
from typing import Dict, Tuple

@dataclass
class Genome:
    params_a: dict
    params_b: dict
    params_c: dict
    confluence: dict
    fitness: float = 0.0
    generation: int = 0
    variant_id: int = 0
    metrics: dict = None

def generate_random_params(ranges: Dict[str, Tuple[float, float]]) -> dict:
    params = {}
    for key, (min_val, max_val) in ranges.items():
        if isinstance(min_val, int) and isinstance(max_val, int):
            params[key] = random.randint(min_val, max_val)
        else:
            params[key] = round(random.uniform(min_val, max_val), 2)
    return params

def normalize_weights(params: dict, prefix: str) -> dict:
    """Ensure weights starting with prefix sum to 1.0"""
    weight_keys = [k for k in params.keys() if k.startswith(prefix)]
    total = sum(params[k] for k in weight_keys)
    if total > 0:
        for k in weight_keys:
            params[k] = round(params[k] / total, 3)
    return params

def random_genome() -> Genome:
    from strategy.lens_a_pullback import LensAPullback
    from strategy.lens_b_breakout import LensBBreakout
    from strategy.lens_c_limitup import LensCLimitUp
    
    pa = generate_random_params(LensAPullback.DEFAULT_PARAMS) if not hasattr(LensAPullback, 'get_param_ranges') else generate_random_params(LensAPullback({}).get_param_ranges())
    pb = generate_random_params(LensBBreakout.DEFAULT_PARAMS) if not hasattr(LensBBreakout, 'get_param_ranges') else generate_random_params(LensBBreakout({}).get_param_ranges())
    pc = generate_random_params(LensCLimitUp.DEFAULT_PARAMS) if not hasattr(LensCLimitUp, 'get_param_ranges') else generate_random_params(LensCLimitUp({}).get_param_ranges())
    
    pa = normalize_weights(pa, "w_")
    pb = normalize_weights(pb, "w_")
    pc = normalize_weights(pc, "w_")
    
    conf = {
        "threshold_a": random.uniform(40, 70),
        "threshold_b": random.uniform(40, 70),
        "high_threshold": random.uniform(65, 85),
        "w_lens_a": random.uniform(0.2, 0.6),
        "w_lens_b": random.uniform(0.2, 0.6),
        "w_lens_c": random.uniform(0.1, 0.4),
    }
    conf = normalize_weights(conf, "w_lens_")
    
    return Genome(params_a=pa, params_b=pb, params_c=pc, confluence=conf)

def crossover(p1: Genome, p2: Genome) -> Genome:
    """Uniform crossover"""
    child_pa = {k: p1.params_a[k] if random.random() > 0.5 else p2.params_a[k] for k in p1.params_a}
    child_pb = {k: p1.params_b[k] if random.random() > 0.5 else p2.params_b[k] for k in p1.params_b}
    child_pc = {k: p1.params_c[k] if random.random() > 0.5 else p2.params_c[k] for k in p1.params_c}
    child_conf = {k: p1.confluence[k] if random.random() > 0.5 else p2.confluence[k] for k in p1.confluence}
    
    return Genome(
        params_a=normalize_weights(child_pa, "w_"),
        params_b=normalize_weights(child_pb, "w_"),
        params_c=normalize_weights(child_pc, "w_"),
        confluence=normalize_weights(child_conf, "w_lens_")
    )

def mutate(genome: Genome, mutation_rate: float = 0.1) -> Genome:
    from strategy.lens_a_pullback import LensAPullback
    from strategy.lens_b_breakout import LensBBreakout
    from strategy.lens_c_limitup import LensCLimitUp
    
    ranges_a = LensAPullback({}).get_param_ranges()
    ranges_b = LensBBreakout({}).get_param_ranges()
    ranges_c = LensCLimitUp({}).get_param_ranges()
    
    def apply_mutation(params, ranges):
        for k, (min_val, max_val) in ranges.items():
            if random.random() < mutation_rate:
                range_span = max_val - min_val
                perturbation = range_span * random.uniform(-0.15, 0.15)
                params[k] = max(min_val, min(max_val, params[k] + perturbation))
                if isinstance(min_val, int): params[k] = int(params[k])
                else: params[k] = round(params[k], 2)
        return params
        
    genome.params_a = apply_mutation(genome.params_a, ranges_a)
    genome.params_b = apply_mutation(genome.params_b, ranges_b)
    genome.params_c = apply_mutation(genome.params_c, ranges_c)
    
    genome.params_a = normalize_weights(genome.params_a, "w_")
    genome.params_b = normalize_weights(genome.params_b, "w_")
    genome.params_c = normalize_weights(genome.params_c, "w_")
    
    return genome
