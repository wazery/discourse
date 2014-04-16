/**
  An input field for a color.

  @param hexValue is a reference to the color's hex value.

  @class Discourse.ColorInputComponent
  @extends Ember.Component
  @namespace Discourse
  @module Discourse
 **/
Discourse.ColorInputComponent = Ember.Component.extend({
  layoutName: 'components/color-input',

  hexValueChanged: function() {
    if (this.get('hexValue')) {
      this.set('hexValue', this.get('hexValue').replace(/[^0-9a-fA-F]/g, ''));
      var hex = this.get('hexValue');
      if (hex.length === 6 || hex.length === 3) {
        if (hex.length === 3) {
          hex = hex.substr(0,1) + hex.substr(0,1) + hex.substr(1,1) + hex.substr(1,1) + hex.substr(2,1) + hex.substr(2,1);
        }
        var brightness = Math.round(((parseInt('0x'+hex.substr(0,2)) * 299) + (parseInt('0x'+hex.substr(2,2)) * 587) + (parseInt('0x'+hex.substr(4,2)) * 114)) /1000),
            style = 'color: ' + (brightness > 125 ? 'black' : 'white') + '; background-color: #' + this.get('hexValue') + ';';
        this.$('input').attr('style', style);
      }
    }
  }.observes('hexValue'),

  didInsertElement: function() {
    var self = this;
    this._super();
    Em.run.schedule('afterRender', function() {
      self.hexValueChanged();
    });
  }
});
