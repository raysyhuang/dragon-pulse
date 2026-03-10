import concurrent.futures
from typing import List
import copy
import random
import logging

from src.evolution.genome import Genome, random_genome, crossover, mutate
from src.evolution.fitness import evaluate_fitness

logger = logging.getLogger(__name__)

def run_evolution(
    train_start: str,
    train_end: str,
    universe: list,
    data_cache: dict,
    csi300_df,
    guardian_config: dict,
    population_size: int = 10,
    generations: int = 5,
    elite_count: int = 2,
    mutation_rate: float = 0.1
) -> List[Genome]:
    """Main Evolution Loop using a Genetic Algorithm."""
    
    logger.info(f"Initializing Population ({population_size} genomes)...")
    population = [random_genome() for _ in range(population_size)]
    
    for gen in range(1, generations + 1):
        logger.info(f"\n--- Generation {gen}/{generations} ---")
        
        # Evaluate fitness in parallel
        # Note: data_cache and csi300_df must be passable to processes or use threads
        # For simplicity and shared memory, we use ThreadPoolExecutor here. 
        # In heavy production, ProcessPoolExecutor with shared memory is better.
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    evaluate_fitness, g, train_start, train_end, universe, data_cache, csi300_df, guardian_config
                ): g for g in population
            }
            
            for future in concurrent.futures.as_completed(futures):
                g = futures[future]
                try:
                    g.fitness = future.result()
                    g.generation = gen
                except Exception as e:
                    logger.error(f"Genome evaluation failed: {e}")
                    g.fitness = 0.0
                    
        # Sort population by fitness
        population.sort(key=lambda x: x.fitness, reverse=True)
        
        best = population[0]
        logger.info(f"Best Fitness: {best.fitness:.4f} | Win Rate: {best.metrics.get('win_rate', 0)*100:.1f}% | Trades: {best.metrics.get('total_trades', 0)}")
        
        if gen == generations:
            break
            
        # Elitism
        next_gen = copy.deepcopy(population[:elite_count])
        
        # Breeding
        while len(next_gen) < population_size:
            # Tournament selection
            tournament = random.sample(population, 3)
            tournament.sort(key=lambda x: x.fitness, reverse=True)
            parent1 = tournament[0]
            
            tournament = random.sample(population, 3)
            tournament.sort(key=lambda x: x.fitness, reverse=True)
            parent2 = tournament[0]
            
            child = crossover(parent1, parent2)
            child = mutate(child, mutation_rate)
            next_gen.append(child)
            
        population = next_gen
        
    return population
