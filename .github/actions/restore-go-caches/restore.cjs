async function restoreAll(inputs, restoreCache) {
  // The standard client runs tar in the workspace. These archives have disjoint
  // paths; start both transfers before awaiting either, and settle both on error.
  const results = await Promise.allSettled(inputs.map(async (input) => {
    const paths = input.path.trim().split('\n');
    const keys = input['restore-keys'].trim().split('\n');
    return restoreCache(paths, input.key, keys);
  }));
  const failures = results.filter(result => result.status === 'rejected');
  if (failures.length) throw new AggregateError(failures.map(result => result.reason), 'Go cache restore failed');
  return results.map(result => result.value);
}

module.exports = { restoreAll };
