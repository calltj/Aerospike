const required = ["MONGO_URI", "PORT"];
required.forEach((v) => {
  if (!process.env[v]) throw new Error(`Missing env var: ${v}`);
});
