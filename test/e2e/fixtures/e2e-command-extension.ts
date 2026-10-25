export default function (pi: {
  registerCommand(name: string, options: {
    description: string;
    handler(args: string): void | Promise<void>;
  }): void;
}) {
  pi.registerCommand("e2e-source-info", {
    description: "Expose stable command source metadata to the gateway E2E suite.",
    handler: async () => {},
  });
}
