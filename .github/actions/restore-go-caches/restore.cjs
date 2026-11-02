async function restoreAll(inputs, restoreCache) {
  // The standard client runs tar in the workspace. These archives have disjoint
  // paths; start both transfers before awaiting either, and settle both on error.
  const results = await Promise.allSettled(inputs.map(async (input) => {
    const paths = input.path.trim().split('\n');
    const keys = (input['restore-keys'] || '').trim().split('\n').filter(Boolean);
    const restored = await restoreCache(paths, input.key, keys);
    if (input.required && restored !== input.key) {
      throw new Error(`Required prepared Go cache missing: ${input.key}; rerun preparation, never compile in a contract shard`);
    }
    return restored;
  }));
  const failures = results.filter(result => result.status === 'rejected');
  if (failures.length) throw new AggregateError(failures.map(result => result.reason), 'Go cache restore failed');
  return results.map(result => result.value);
}

module.exports = { restoreAll };
