migrate((app) => {
  const collection = new Collection({
    name: "rooms",
    type: "base",
    fields: [
      { name: "state", type: "json" },
      { name: "created", type: "autodate", onCreate: true },
    ],
    listRule: "",
    viewRule: "",
    updateRule: "",
  });
  app.save(collection);
}, (app) => {
  app.delete(app.findCollectionByNameOrId("rooms"));
});
