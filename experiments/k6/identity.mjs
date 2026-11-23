export function deterministicUUID(value) {
  function hash32(input, salt) {
    let hash = (2166136261 ^ salt) >>> 0;
    for (let index = 0; index < input.length; index += 1) {
      hash ^= input.charCodeAt(index);
      hash = Math.imul(hash, 16777619) >>> 0;
    }
    return hash.toString(16).padStart(8, "0");
  }

  let hex = [1, 2, 3, 4].map((salt) => hash32(value, salt)).join("");
  hex = `${hex.slice(0, 12)}4${hex.slice(13)}`;
  const variantNibble = ((parseInt(hex[16], 16) & 3) | 8).toString(16);
  hex = `${hex.slice(0, 16)}${variantNibble}${hex.slice(17)}`;
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function acceptedBenchmarkStatus(variant, status) {
  return status === 202 || (variant === "gateway" && status === 409);
}
