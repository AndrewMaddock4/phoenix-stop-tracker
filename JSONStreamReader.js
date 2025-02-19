import EventEmmiter from "events";

export default class JSONStreamReader extends EventEmmiter {
  #stream;
  #currentSlice = "";
  constructor(stream) {
    super();
    this.#stream = stream;
    this.#stream.on("data", (chunk) => this.#processJson(chunk));
    this.#stream.on("end", () => this.emit("end"));
  }

  /**
   *
   * @param {string | Buffer} data
   */
  #processJson(data) {
    /**
     * As data is read from the file stream, This method will concatenate chunks of it
     * until a JSON object is successfully parsed. This object is sent as an event to anyting listening
     */
    this.#currentSlice += data.toString();

    let slices = this.#currentSlice.split("\n");
    this.#currentSlice = "";

    for (const slice of slices) {
      try {
        let jsonSlice = JSON.parse(slice.trim());
        this.emit("data", jsonSlice);
      } catch (error) {
        this.#currentSlice += slice.trim();
      }
    }
  }
}
