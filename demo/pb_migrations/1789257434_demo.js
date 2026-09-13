migrate((app) => {
  const collection = new Collection({
    name: "demo",
    type: "base",
    fields: [{ name: "state", type: "json" }],
    listRule: "",
    viewRule: "",
    updateRule: "",
  });
  app.save(collection);

  const record = new Record(collection);
  record.set("id", "demo00000000000");
  record.set("state", {});
  app.save(record);
}, (app) => {
  app.delete(app.findCollectionByNameOrId("demo"));
});
