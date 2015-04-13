const TYPE_SPLITTER = /^(String|Number|Binary)(?:\.([a-zA-Z0-9]))?$/;

class AttributeMap {
  constructor (json) {
    this.by_name = new Map();
    if (json && typeof json === 'object') {
      for (let name of Object.keys(json)) {
        let by_type = json[name];
        for (let type of Object.keys(by_type)) {
          this.set(name, type, by_type[type]);
        }
      }
    }
  }

  has(name, type, value) {
    name = String(name);
    type = String(type);
    if (!this.by_name.has(name)) {
      return false;
    }
    let by_type = this.by_name.get(name); 
    return by_type.has(type);
  }

  get(name, type, value) {
    name = String(name);
    type = String(type);
    if (!this.by_name.has(name)) {
      return undefined;
    }
    let by_type = this.by_name.get(name); 
    return by_type.get(type, value);
  }

  set(name, type, value) {
    name = String(name);
    type = String(type);
    if (!this.by_name.has(name)) {
      this.by_name.set(name, new Map());
    }
    let by_type = this.by_name.get(name); 
    by_type.set(type, value);
  }

  toJSON() {
    let value = this.value;
    if (this.type == 'Binary') {
      value = new Buffer(val).toString('base64');
    }
    let ret = Object.create(null);
    for (let [name,by_type] of this.by_name.entries()) {
      let for_name = Object.create(null);
      ret[name] = for_name;
      for (let [type,value] of by_type.entries()) {
        for_name[type] = value; 
      }
    }
    return {
      type: this.type,
      tag: this.tag,
      value 
    }
  }
}
module.exports = Attribute;
